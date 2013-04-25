package topicus.databases;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;

import topicus.benchmarking.BenchmarksScript;

public class VerticaDatabase extends AbstractDatabase {
	protected Random random;
	
	public VerticaDatabase () {
		super();
		
		this.name = "exploitatie";
		this.user = "dbadmin";
		this.password = "test";
		this.port = 5433;	
		
		this.random = new Random();
	}
	
	public String getJdbcDriverName() {
		return "com.vertica.jdbc.Driver";
	}

	public String getJdbcUrl() {
		return "jdbc:vertica://";
	}
	
	public boolean canFetchRealResults () {
		return true;
	}
	
	public void setConnectionQueryTimeout (Connection conn, int timeout) throws SQLException {
		Statement q = conn.createStatement();
		q.execute("SET SESSION RUNTIMECAP '" + timeout + " MILLISECOND';");
	}
	
	public int fetchRealResults (Connection conn, BenchmarksScript benchmark) throws SQLException {
		String benchmarkId = benchmark.getID().replaceAll("-",  "");
		
		int fetchCount = 0;
			Statement q = conn.createStatement();
			
			// fetch all labelled queries from request log
			ResultSet results = q.executeQuery(
				"SELECT REQUEST_LABEL, REQUEST_DURATION_MS, message " +
				"FROM QUERY_REQUESTS " +
				"LEFT JOIN error_messages " +
				"	ON query_requests.statement_id = error_messages.statement_id " +
				"	AND query_requests.transaction_id = error_messages.transaction_id " +
				" 	AND query_requests.session_id = error_messages.session_id " +
				"	AND query_requests.request_id = error_messages.request_id " + 
				"	AND error_messages.message LIKE '%exceeded run time cap%' " +
				"WHERE REQUEST_LABEL LIKE '" + benchmarkId + "%' " +
				"GROUP BY request_label, request_duration_ms, message " +
				"ORDER BY REQUEST_LABEL ASC;"
			);
			
			int[] times = new int[4];
			while(results.next()) {
				String label = results.getString("REQUEST_LABEL");
				String split[] = label.split("_");
				int userId = Integer.parseInt(split[1]);
				int iteration = Integer.parseInt(split[2]);
				int setId = Integer.parseInt(split[3]);
				int queryId = Integer.parseInt(split[4]);
				
				int time = results.getInt("REQUEST_DURATION_MS");
				
				// if error message length is not null
				// then query has been timed out
				if (results.getString("message") != null) {
					time = BenchmarksScript.QUERY_TIMEOUT;
				}
				
				if (queryId < 4) {
					times[queryId] = time;
				} else {
					benchmark.addResult(userId, iteration, setId, times[1], times[2], times[3], time);
					fetchCount++;
				}				
			}				
		
		return fetchCount;
	}
	
	public String addQueryLabel (String query, String benchmarkId, int userId, int iteration, int setId, int queryId) {
		benchmarkId = benchmarkId.replaceAll("-", "");
		String label = benchmarkId + "_" + userId + "_" + iteration + "_" + setId + "_" + queryId; 
				
		query = query.replaceFirst("SELECT", "SELECT /*+label(" + label + ")*/");
		
		return query;
	}

	public int getNodeCount(Connection conn) throws SQLException {	
		Statement stmt = conn.createStatement();
		ResultSet result = null;
		
		result = stmt.executeQuery("SELECT COUNT(*) AS node_count FROM nodes WHERE node_state = 'UP';");
		
		// retrieve node count
		result.next();
		int nodeCount = result.getInt("node_count");
		
		// close result
		result.close();
		stmt.close();
		
		return nodeCount;
	}
	
	public int deployData(Connection conn, String fileName, String tableName)
			throws SQLException {
		Statement q = conn.createStatement();
		int rows = q.executeUpdate("COPY " + tableName + " from '" + fileName + "' delimiter '#'" +
				" null as 'NULL' abort on error DIRECT;");
		return rows;
	}
	
	public void createTable(Connection conn, String tableName, ArrayList<DbColumn> columns, String partitionBy) throws SQLException {
		// construct query
		String query = "";
		
		query += "CREATE TABLE " + tableName + " (";
		
		ArrayList<String> colSql = new ArrayList<String>();

		for(DbColumn col : columns) {
			String colQuery = "";
			
			colQuery += col.getName() + " ";
			
			switch(col.getType()) {
				case INTEGER:
					colQuery += "integer";
					break;
				case SMALL_INTEGER:
					colQuery += "smallint";
					break;
				case VARCHAR:
					colQuery += "character";
					colQuery += " varying(" + col.getLength() + ")";
					break;
				case TIMESTAMP_WITHOUT_TIMEZONE:
					colQuery += "timestamp without time zone";
					break;
				case DOUBLE:
					colQuery += "numeric";
					break;
				default:
					throw new SQLException("Unknown column type: " + col.getType());
			}
						
			if (col.isPrimaryKey()) {
				colQuery += " primary key";
			} else if (col.isUnique()) {
				colQuery += " unique";
			} else if (col.isForeignKey()) {
				colQuery += " references " + col.getForeignTable() + "(" + col.getForeignColumn() + ")";
			}
			
			if (col.allowNull() == false) {
				colQuery += " not null";
			}		
			
			colSql.add(colQuery);
		}
		
		query += StringUtils.join(colSql, ", ");
		
		query += ")";
		
		if (partitionBy != null) {
			query += " partition by " + partitionBy;
		}
		
		query += ";";

		Statement q = conn.createStatement();
		q.execute(query);
		
		q.close();
	}
	
	public void dropTable(Connection conn, String tableName) throws SQLException {
		Statement q = conn.createStatement();
		q.execute("DROP TABLE IF EXISTS " + tableName + " CASCADE;");
	}

	@Override
	public void runBenchmarkQuery(List<Connection> conns, String query, String benchmarkId, int userId,
			int iteration, int setId, int queryId) throws SQLException, TimeoutException {
		// add label to query
		benchmarkId = benchmarkId.replaceAll("-", "");
		String label = benchmarkId + "_" + userId + "_" + iteration + "_" + setId + "_" + queryId; 
				
		query = query.replaceFirst("SELECT", "SELECT /*+label(" + label + ")*/");
		
		// grab a random connection
		int myConnIndex = random.nextInt(conns.size());
		
		Connection conn = conns.get(myConnIndex);
		
		synchronized(conn) {					
			// execute query
			Statement stmt = null;
			ResultSet result = null;
			try {
				stmt = conn.createStatement();
				result = stmt.executeQuery(query);
			} catch (SQLException e) {
				String msg = e.getMessage().toLowerCase();
				if (msg.indexOf("execution time exceeded run time cap") > -1) {
					throw new TimeoutException();
				} else {
					throw e;
				}
			} finally {
				if (stmt != null) stmt.close();
				if (result != null) result.close();
			}
		}
		
		
	}

}
