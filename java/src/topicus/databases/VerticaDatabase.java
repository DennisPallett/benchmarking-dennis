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
	
	
	public VerticaDatabase () {
		super();
		
		this.name = "exploitatie";
		this.user = "dbadmin";
		this.password = "test";
		this.port = 5433;	
	}
	
	public String getJdbcDriverName() {
		return "com.vertica.jdbc.Driver";
	}

	public String getJdbcUrl() {
		return "jdbc:vertica://";
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
		
		
		
	}

}
