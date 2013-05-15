package topicus.databases;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import topicus.benchmarking.AbstractBenchmarkRunner;
import topicus.benchmarking.BenchmarksScript;
import topicus.benchmarking.JdbcBenchmarkRunner;
import topicus.benchmarking.AbstractBenchmarkRunner.PrepareException;

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
	
	public AbstractBenchmarkRunner createBenchmarkRunner () {
		return new BenchmarkRunner();
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
	
	public int[] deployData(Connection conn, String fileName, String tableName)
			throws SQLException {
		Statement q = conn.createStatement();
		
		long start = System.currentTimeMillis();
		int rows = q.executeUpdate("COPY " + tableName + " from '" + fileName + "' delimiter '#'" +
				" null as 'NULL' abort on error DIRECT;");
		int runTime = (int) (System.currentTimeMillis() - start);
		
		return new int[]{runTime, rows};
	}
	
	public void createTable(Connection conn, DbTable table) throws SQLException {
		String tableName = table.getName();
		ArrayList<DbColumn> columns = table.getColumns();
		String partitionBy = table.partitionBy();
		String orderBy = table.orderBy();
		
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
			
			if (col.isTenantKey()) {
				colQuery += " ENCODING RLE";
			}
			
			colSql.add(colQuery);
		}
		
		query += StringUtils.join(colSql, ", ");
		
		query += ")";
		
		if (orderBy != null) {
			query += " ORDER BY " + orderBy;
		}
		
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

	public class BenchmarkRunner extends AbstractBenchmarkRunner {
		protected Connection conn;
		protected Statement statement;
		
		protected TimeoutWatcher timeout;
		
		protected long startTime = -1;
		protected boolean isCancelled = false;

		@Override
		public void prepareBenchmark() throws PrepareException {
			try {
				this.setupConnection();
			} catch (Exception e) {
				throw new PrepareException(e.getClass().getName() + ": " + e.getMessage());
			}	
			
			timeout = new TimeoutWatcher(this);
		}

		@Override
		public void runIteration(int iteration) {
			Iterator<String[]> iter = this.queryList.iterator();
			
			int setNum = 1;
			while(iter.hasNext()) {
				String[] queries = iter.next();
				
				String sql = "";
				for(String query : queries) {
					query = query.trim();
					if (query.endsWith(";") == false) {
						query += ";";
					}
					
					sql += query;
				}
				
				sql = this._replaceOrgId(sql);
				
				boolean failed = false;
				
				try {
					isCancelled = false;
					startTime = System.currentTimeMillis();
					statement.execute(sql);

				} catch (SQLException e) {
					if (isCancelled == false) {
						e.printStackTrace();
						failed = true;
					}
				}
							
				int setTime = (int) (System.currentTimeMillis() - startTime);
				int queryTime = setTime/4;
				
				if (isCancelled) {
					setTime = BenchmarksScript.QUERY_TIMEOUT;
					queryTime = BenchmarksScript.QUERY_TIMEOUT;
				}
				
				// save results
				if (failed == false) {
					this.owner.addResult(
						this.userId, 
						iteration, 
						setNum,
						queryTime,
						queryTime,
						queryTime,
						queryTime,
						setTime
					);
				}
				
				setNum++;
				
				// rate limiting
				// to avoid flooding the database
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}

		@Override
		public void finishBenchmark() {
			// stop timeout watcher
			this.timeout.interrupt();
			
			this.owner.printLine("User #" + this.userId + " closing connection");
			
			try {
				statement.close();
			} catch (SQLException e) {
				// don't care
			}
			
			try {
				conn.close();
			} catch (SQLException e) {
				// don't care
			}
					
			this.owner.printLine("Connection for user #" + this.userId + " closed");			
		}
		
		public void checkTimeout () {
			long currTime = System.currentTimeMillis();
			if (startTime > -1 && currTime - startTime >= BenchmarksScript.QUERY_TIMEOUT) {
				try {
					isCancelled = true;
					statement.cancel();		
					startTime = -1;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		protected void setupConnection () throws SQLException {
			// determine node
			int node = (this.userId % this.nodes) + 1;
						
			this.owner.printLine("Setting up connection for user #" + this.userId + " to node" + node);
						
			// create Connection
			conn = this.database.setupConnection("node" + node);
						
			// create statement
			statement = conn.createStatement();
		}
		
	}
	
	public class TimeoutWatcher extends Thread {
		protected BenchmarkRunner owner;
		
		public TimeoutWatcher (BenchmarkRunner owner) {
			this.owner = owner;
		}
		
		public void run () {
			// standard sleep granularity is 1000 ms
			int sleep = 1000;
			
			// however standard sleep might be too high for very low
			// time-outs. check this:
			if (BenchmarksScript.QUERY_TIMEOUT < sleep) {
				sleep = BenchmarksScript.QUERY_TIMEOUT / 2;
			}
			
			while(true) {
				try {
					// wait certain number before checking again
					Thread.sleep(sleep);
					
					// check for timed-out queries
					owner.checkTimeout();		
				} catch (InterruptedException e) {
					// don't care!
				}
			}
		}
		
	}

}
