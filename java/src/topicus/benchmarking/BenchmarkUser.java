package topicus.benchmarking;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class BenchmarkUser extends Thread {
	protected Random random;
	
	protected int userId;
	protected List<Connection> conns = new ArrayList<Connection>();
	
	protected List<String[]> queryList;
	protected int iterations;
	protected int nodes;
	protected int numberOfUsers;
		
	protected Benchmarks owner;
	
	protected boolean isReady = false;
	protected boolean isFailed = false;
	protected boolean isFinished = false;
	
	protected Exception failException = null;
	
	public BenchmarkUser (Benchmarks owner, int userId, List<String[]> queryList, 
			int iterations, int nodes) throws SQLException {
		this.owner = owner;
		this.userId = userId;
		this.queryList = queryList;
		this.iterations = iterations;
		this.nodes = nodes;
		
		this.random = new Random();
	}
	
	public boolean isReady () {
		return this.isReady;
	}
	
	public boolean isFailed () {
		return this.isFailed;
	}
	
	public Exception getFailException () {
		return this.failException;
	}
	
	public void run () {		
		try {
			this.setupConnections();
			
			this.owner.printLine("User #" + this.userId + " ready and waiting");
			this.isReady = true;
			
			synchronized(this.owner) {
				try {
					this.owner.wait();
				} catch(InterruptedException e){
					
				}
			}
			
			try {
				this.runBenchmarks();
			} catch (InterruptedException e) {
				this.isFailed = true;
				this.failException = e;
				this.owner.printError("User #" + this.userId + " failed during benchmarking");
			}
		} catch (SQLException e) {
			this.isFailed = true;
			this.failException = e;
			this.owner.printError("User #" + this.userId + " failed");
		}		
	}
	
	protected void runBenchmarks () throws InterruptedException {
		this.owner.printLine("User #" + this.userId + " starting benchmarks");
		
		for(int i=1; i <= this.iterations; i++) {
			this.owner.printLine("User #" + this.userId + " running iteration " + i);
			this.runIteration(i);
			this.owner.printLine("User #" + this.userId + " finished iteration " + i);
		}
	}
	
	protected void runIteration (int iteration) throws InterruptedException {
		Iterator<String[]> iter = this.queryList.iterator();
		
		int setNum = 1;
		while(iter.hasNext()) {
			String[] queries = iter.next();
			int[] times = {0,0,0,0};
			
			long start = System.currentTimeMillis();
			
			List<Thread> threads = new LinkedList<Thread>();
			List<QueryRunner> queryRunners = new LinkedList<QueryRunner>();			
			
			// start query threads
			for(int j = 0; j < queries.length; j++) {
				QueryRunner qr = new QueryRunner(j+1, queries[j]);
				Thread t = new Thread(qr);
				t.start();
				threads.add(t);
				queryRunners.add(qr);
			}
			
			// get execution time for each query
			for(int t = 0; t < threads.size(); t++) {
				// Wait for thread to finish
				threads.get(t).join();
				QueryRunner queryRunner = queryRunners.get(t);
				QueryResult queryResult = queryRunner.result;
				
				if (queryResult.failed == false) {
					times[queryRunner.id-1]= queryResult.runtime; 
				}
			}
			
			// calculate time for complete set of queries
			int setTime = (int) (System.currentTimeMillis() - start);
			
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
		}
	}
	
	public void setupConnections () throws SQLException {
		this.owner.printLine("Setting up connections for user #" + this.userId);
		
		// setup connections
		for(int i=1; i <= this.nodes; i++) {			
			// setup connection string
			String url = this.owner.getJdbcUrl() + "node" + i + ":" + this.owner.dbPort + "/" + this.owner.dbName;
			this.owner.printLine("Setting up connection #" + i + ": " + url + " for user #" + this.userId);
						
			// setup connection
			Connection conn = DriverManager.getConnection(url, this.owner.dbUser, this.owner.dbPassword);
			
			// add connection to list of connections
			this.conns.add(conn);
			
			this.owner.printLine("Connection #" + i + " setup for user #" + this.userId);
		}
	}
	
	protected void executeQuery(String query) throws Exception {
		// grab a random connection
		int myConnIndex = random.nextInt(this.nodes);
		
		// grab next connection
		//int myConnIndex = connIndex++ % this.conns.size();
				
		Connection conn = this.conns.get(myConnIndex);
		
		query = this._replaceOrgId(query);
		
		synchronized(conn) {					
			// execute query
			Statement stmt = null;
			ResultSet result = null;
			try {
				stmt = conn.createStatement();
				result = stmt.executeQuery(query);
			} catch (SQLException e) {
				throw e;
			} finally {
				if (stmt != null) stmt.close();
				if (result != null) result.close();
			}
		}
	}
	
	protected String _replaceOrgId(String query) {
		// replace all organisation ID's with tenant's org ids
				
		// number of rows in the organisation table
		final int ORG_ROW_COUNT = 988;
		
		// id's in queries
		int[] ids = {752, 756, 799};
		
		// replace each ID
		for(int id : ids) {
			query = query.replaceAll(String.valueOf(id), String.valueOf((this.userId-1) * ORG_ROW_COUNT + id));
		}		
		
		return query;
	}
	
	public class QueryRunner implements Runnable {
		public QueryResult result;
		
		private int id;

		public QueryRunner(int id, String query) {
			QueryResult result = new QueryResult(query);
			this.id = id;
			this.result = result;
		}

		public void run() {
			// Now execute the query
			try {
				long start = System.currentTimeMillis();
				BenchmarkUser.this.executeQuery(this.result.query);
				int runTime = (int) (System.currentTimeMillis() - start);
				this.result.runtime = runTime;
			} catch(Exception e) {
				BenchmarkUser.this.owner.printError("Error in query #" + this.id + ": " + e.getMessage());
				this.result.failed = true;
			}
		}
	}
	
	public class QueryResult {
		public int runtime;
		public String query;
		public boolean failed = false;

		public QueryResult(String query) {
			this.query = query;
		}

		public QueryResult(String query, int runtime) {
			this.query = query;
			this.runtime = runtime;
		}
	}

}
