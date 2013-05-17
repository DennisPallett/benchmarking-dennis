package topicus.databases;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import topicus.benchmarking.AbstractBenchmarkRunner;
import topicus.benchmarking.BenchmarksScript;
import topicus.benchmarking.JdbcBenchmarkRunner;
import topicus.benchmarking.JdbcBenchmarkRunner.QueryResult;
import topicus.benchmarking.JdbcBenchmarkRunner.QueryRunner;

public class VoltdbDatabase extends AbstractDatabase {	
	public VoltdbDatabase () {
		super();
		
		this.name = "";
		this.user = "";
		this.password = "";
		this.port = 21212;	
	}
	
	public AbstractBenchmarkRunner createBenchmarkRunner () {
		return new BenchmarkRunner();
	}
	
	public void close () {

	}
	
	public String getJdbcDriverName() {
		return "org.voltdb.jdbc.Driver";
	}

	public String getJdbcUrl() {
		return "jdbc:voltdb://";
	}
		
	public int getNodeCount(Connection conn) throws SQLException {	
		Statement stmt = conn.createStatement();
		ResultSet results = null;
		
		CallableStatement proc = conn.prepareCall("{call @SystemInformation(?)}");
		proc.setString(1,  "DEPLOYMENT");
		results = proc.executeQuery();
		
		int nodeCount = 0;
		while(results.next()) {
			String property = results.getString(1);
			String value = results.getString(2);
			
			if (property.equals("hostcount")) {
				nodeCount = Integer.parseInt(value);
			}
		}
		
		results.close();
		stmt.close();
		
		return nodeCount;
	}
	
	public int[] deployData(Connection conn, String fileName, String tableName) throws SQLException {
		long offset = 0;
		
		int totalRowCount = 0;
		int runTime = 0;
		
		NumberFormat formatter = NumberFormat.getInstance();
		DecimalFormat df = new DecimalFormat("#.##");
		
		long startTime = System.currentTimeMillis();
		while(offset > -1) {
			CallableStatement proc = conn.prepareCall("{call BulkLoad(?, ?, ?)}");
			proc.setString(1, tableName);
			proc.setString(2, fileName);
			proc.setLong(3,  offset);
			
			ResultSet results = proc.executeQuery();
			
			results.next();
			
			offset = results.getLong("offset"); 
			int rowCount = results.getInt("rowcount");
			double execTime = (double)results.getInt("exectime")/1000;			
			
			totalRowCount = totalRowCount + rowCount;
			
			if (rowCount > 0) {
				printLine("Inserted " + formatter.format(rowCount) + " rows (total row count: " + formatter.format(totalRowCount) + ") " +
					"in approx. " + df.format(execTime) + " seconds");
			}
			
			results.close();
			proc.close();
		}	
		
		runTime = (int) ((int) System.currentTimeMillis() - startTime);
		
		return new int[]{runTime, totalRowCount};
	}
		
	public void createTable(Connection conn, DbTable table) throws SQLException {
		throw new SQLException("Not supported by VoltDB");
	}
	
	public void dropTable(Connection conn, String tableName) throws SQLException {
		throw new SQLException("Not supported by VoltDB");
	}
	
	public class BenchmarkRunner extends AbstractBenchmarkRunner {
		protected Client client;
		
		protected Client[] clients = new Client[NR_OF_QUERIES];

		@Override
		public void prepareBenchmark() throws PrepareException {
			try {
				this.setupClient();
			} catch (Exception e) {
				throw new PrepareException(e.getClass().getName() + ": " + e.getMessage());
			}		
		}

		@Override
		public void runIteration(int iteration) {
			Iterator<String[]> iter = this.queryList.iterator();
			
			int setNum = 1;
			while(iter.hasNext()) {
				String[] setInfo = iter.next();
				String procName = setInfo[0];
				int yearKey = Integer.parseInt(setInfo[1]);
				String parentId = setInfo[2];
				
				parentId = this._replaceOrgId(parentId);
				
				int parentIdNum = Integer.parseInt(parentId);
				
				int tenantYearKey = (this.userId * 10000) + yearKey;
				
				int[] times = {0,0,0,0};
				
				List<Thread> threads = new LinkedList<Thread>();
				List<QueryRunner> queryRunners = new LinkedList<QueryRunner>();	
				
				int startId = 1;
				if (procName.toLowerCase().equals("set1") == false) {
					startId = 5;
				}
				
				// create queryRunners and separate threads
				for(int i=startId; i < startId+4; i++) {
					QueryRunner qr = new QueryRunner(i, tenantYearKey, parentIdNum);
					queryRunners.add(qr);
					Thread t = new Thread(qr);
					threads.add(t);
				}
				
				// start all queries
				long startTime = System.currentTimeMillis();
				for(int j=0; j < threads.size(); j++) {
					threads.get(j).start();
				}
				
				// get execution time for each query
				for(int t = 0; t < queryRunners.size(); t++) {
					// Wait for thread to finish
					try {
						threads.get(t).join();
					} catch (InterruptedException e) {
						// don't care
					}
										
					QueryRunner queryRunner = queryRunners.get(t);
					
					if (queryRunner.failed == false) {
						times[queryRunner.getQueryId() % NR_OF_QUERIES] = queryRunner.runTime; 
					}
					
					if (queryRunner.runTime == BenchmarksScript.QUERY_TIMEOUT) {
						owner.printError("Query #" + t + " of set #" + setNum + " timed out");
					}
				}				
							
				int setTime = (int) (System.currentTimeMillis() - startTime);

				// save results
				this.owner.addResult(
					this.userId, 
					iteration, 
					setNum,
					times[0],
					times[1],
					times[2],
					times[3],
					setTime
				);
				
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
			this.owner.printLine("User #" + this.userId + " closing clients");
			for(int i=0; i < NR_OF_QUERIES; i++) {
				try {
					clients[i].close();
				} catch (InterruptedException e) {
					// don't care
				}
			}
			this.owner.printLine("All clients for user #" + this.userId + " closed");					
		}
		
		protected int executeQuery(int queryId, int tenantYearKey, int parentId) throws Exception {					
			// execute query
			int runTime = 0;
						
			Client client = clients[queryId % NR_OF_QUERIES];
					
			long startTime = System.currentTimeMillis();
			ClientResponse response = null;
			
			String procName = "Query" + queryId;
			
			try {
				response = client.callProcedure(procName,  tenantYearKey, parentId);				
				runTime = (int) ((int) System.currentTimeMillis() - startTime);
			} catch (ProcCallException e) {
				response = e.getClientResponse();
				if (response != null && response.getStatus() == ClientResponse.CONNECTION_TIMEOUT) {
					runTime = BenchmarksScript.QUERY_TIMEOUT;
				} else {
					throw e;
				}
			} catch (Exception e) {
				throw e;					
			}
			
			return runTime;
		}
		
		protected void setupClient () throws UnknownHostException, IOException {
			// setup config
			ClientConfig config = new ClientConfig(null, null);
			config.setProcedureCallTimeout(BenchmarksScript.QUERY_TIMEOUT);
			config.setConnectionResponseTimeout(BenchmarksScript.QUERY_TIMEOUT);
						
			this.owner.printLine("Setting up clients for user #" + this.userId);
			
			// setup NR_OF_QUERIES connections (1 for each query)
			for(int i=0; i < NR_OF_QUERIES; i++) {
				// determine node
				int node = (i % this.nodes) + 1;
				
				this.owner.printLine("Setting up client #" + (i+1) + " to node" + node + " for user #" + this.userId);
				
				// create Client object
				clients[i] = ClientFactory.createClient(config);
							
				// create connection
				clients[i].createConnection("node" + node);
							
				this.owner.printLine("Client #" + (i+1) + " setup for user #" + this.userId);
			}
			
		}
		
		public class QueryRunner implements Runnable {		
			private int queryId;
			private int tenantYearKey;
			private int parentId;
			
			public boolean failed = false;
			public int runTime;

			public QueryRunner(int queryId, int tenantYearKey, int parentId) {			
				this.queryId = queryId;
				this.tenantYearKey = tenantYearKey;
				this.parentId = parentId;
				
			}
			
			public int getQueryId () {
				return queryId;
			}

			public void run() {		
				// Now execute the query
				try {
					runTime = BenchmarkRunner.this.executeQuery(this.queryId, this.tenantYearKey, this.parentId);
				} catch(Exception e) {
					BenchmarkRunner.this.owner.printError("Error in query #" + this.queryId + ": " + e.getMessage());
					failed = true;
				}
			}
		}
				
	}



}
