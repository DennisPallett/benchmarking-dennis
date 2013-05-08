package topicus.benchmarking;

import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import topicus.databases.AbstractDatabase;
import topicus.databases.AbstractDatabase.TimeoutException;

public class BenchmarkUser extends Thread {
	public final static int NR_OF_QUERIES = 4;
	
	protected Random random;	
	
	protected int userId;
	//protected List<Connection> conns = new ArrayList<Connection>();
	protected Connection[] conns = new Connection[NR_OF_QUERIES];
	protected Statement[] statements = new Statement[NR_OF_QUERIES];
	protected long[] startTimes = new long[NR_OF_QUERIES];
	protected boolean[] isCancelled = new boolean[NR_OF_QUERIES];
	
	protected List<String[]> queryList;
	protected int iterations;
	protected int nodes;
	protected int numberOfUsers;
	protected int numberOfTenants;
		
	protected BenchmarksScript owner;
	protected AbstractDatabase database;
	
	protected boolean isReady = false;
	protected boolean isFailed = false;
	protected boolean isFinished = false;
		
	protected Exception failException = null;
	
	protected TimeoutWatcher timeoutWatcher = null;
	
	public BenchmarkUser (BenchmarksScript owner, int userId, List<String[]> queryList, 
			int iterations, int nodes, int numberOfTenants, AbstractDatabase database) throws SQLException {
		this.owner = owner;
		this.userId = userId;
		this.queryList = queryList;
		this.iterations = iterations;
		this.nodes = nodes;
		this.numberOfTenants = numberOfTenants;
		this.database = database;
		
		startTimes[0] = -1;
		startTimes[1] = -1;
		startTimes[2] = -1;
		startTimes[3] = -1;
				
		this.timeoutWatcher = new TimeoutWatcher(this);
		
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
		
		// close all statements and connections
		this.owner.printLine("User #" + this.userId + " closing connections");
		for(int i=0; i < NR_OF_QUERIES; i++) {
			try {
			statements[i].close();
			} catch (SQLException e) { 
				// don't care
			}
			try {
				conns[i].close();
			} catch (SQLException e) {
				// don't care
			}
		}
		this.owner.printLine("All connections for user #" + this.userId + " closed");
	}
	
	protected void runBenchmarks () throws InterruptedException {
		this.owner.printLine("User #" + this.userId + " starting benchmarks");
		
		// start timeout watcher
		this.timeoutWatcher.start();
		
		for(int i=1; i <= this.iterations; i++) {
			this.owner.printLine("User #" + this.userId + " running iteration " + i);
			this.runIteration(i);
			this.owner.printLine("User #" + this.userId + " finished iteration " + i);
		}
		
		// stop timeout watcher
		this.timeoutWatcher.interrupt();
	}
	
	protected void runIteration (int iteration) throws InterruptedException {
		Iterator<String[]> iter = this.queryList.iterator();
		
		int setNum = 1;
		while(iter.hasNext()) {
			String[] queries = iter.next();
			int[] times = {0,0,0,0};
						
			List<Thread> threads = new LinkedList<Thread>();
			List<QueryRunner> queryRunners = new LinkedList<QueryRunner>();	
						
			// create query threads
			for(int j = 0; j < queries.length; j++) {
				QueryRunner qr = new QueryRunner(queries[j], iteration, setNum, j+1);
				Thread t = new Thread(qr);
				threads.add(t);
				queryRunners.add(qr);
			}
			
			// start all queries
			long startTime = System.currentTimeMillis();
			for(int j=0; j < threads.size(); j++) {
				threads.get(j).start();
			}
			
			// get execution time for each query
			for(int t = 0; t < queryRunners.size(); t++) {
				// Wait for thread to finish
				threads.get(t).join();
				QueryRunner queryRunner = queryRunners.get(t);
				QueryResult queryResult = queryRunner.result;
				
				if (queryResult.failed == false) {
					times[queryRunner.getQueryId() -1]= queryResult.runtime; 
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
			Thread.sleep(100);
		}
	}
	
	public void setupConnections () throws SQLException {
		this.owner.printLine("Setting up connections for user #" + this.userId);
		
		// setup NR_OF_QUERIES connections (1 for each query)
		for(int i=0; i < NR_OF_QUERIES; i++) {
			// determine node
			int node = (i % this.nodes) + 1;
			
					this.owner.printLine("Setting up connection #" + (i+1) + " to node" + node + " for user #" + this.userId);
			
			this.conns[i] = database.setupConnection("node" + node);
						
			this.owner.printLine("Connection #" + (i+1) + " setup for user #" + this.userId);
		}
	}
	
	protected int executeQuery(String query, int iteration, int setId, int queryId) throws SQLException {	
		// replace proper ORG ID's
		query = this._replaceOrgId(query);
			
		// execute query
		ResultSet result = null;
		int runTime = 0;
			
		PreparedStatement stmt = null;
		
		Connection conn = this.conns[queryId-1];
		
		if (query.toLowerCase().indexOf("call:") > -1) {
			String procName = query.substring(5, query.indexOf(":", 5));
			String args[] = query.substring(("CALL:" + procName + ":").length()).split(":");
			
			String procQuery = "{call " + procName + "(";        	
        	for(String arg : args) {
        		procQuery += "?, ";
        	}
        	procQuery = procQuery.substring(0, procQuery.length()-2);
        	procQuery += ")}";
        	
			CallableStatement proc = conn.prepareCall(procQuery);
			
			for(int i=0; i < args.length; i++) {
				proc.setString(i+1,  args[i]);
			}
			
			this.statements[queryId-1] = proc;			
		} else {		
			this.statements[queryId-1] = conn.prepareStatement(query);
		}	
				
		stmt = (PreparedStatement) this.statements[queryId-1];
				
		try {
			isCancelled[queryId-1] = false;
			startTimes[queryId-1] = System.currentTimeMillis();
			result = stmt.executeQuery();
			runTime = (int) ((int) System.currentTimeMillis() - startTimes[queryId-1]);
			startTimes[queryId-1] = -1;
		} catch (SQLException e) {
			if (isCancelled[queryId-1]) {
				runTime = BenchmarksScript.QUERY_TIMEOUT;
			} else {
				throw e;
			}
		} finally {
			if (result != null) result.close();
		}
		
		return runTime;
	}
	
	public void checkTimeouts () {
		long currTime = System.currentTimeMillis();
		for(int i=0; i < NR_OF_QUERIES; i++) {
			if (startTimes[i] > -1 && currTime - startTimes[i] >= BenchmarksScript.QUERY_TIMEOUT) {
				try {
					isCancelled[i] = true;
					statements[i].cancel();
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	protected String _replaceOrgId(String query) {
		// replace all organisation ID's with tenant's org ids
				
		// number of rows in the organisation table
		final int ORG_ROW_COUNT = 988;
		
		// id's in queries
		int[] ids = {752, 755, 756, 799};
		
		// replace each ID
		for(int id : ids) {
			query = query.replaceAll(String.valueOf(id), String.valueOf((this.userId-1) * ORG_ROW_COUNT + id));
		}		
		
		return query;
	}
	
	public class TimeoutWatcher extends Thread {
		protected BenchmarkUser owner;
		
		public TimeoutWatcher (BenchmarkUser owner) {
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
					owner.checkTimeouts();		
				} catch (InterruptedException e) {
					// don't care!
				}
			}
		}
		
	}
	
	public class QueryRunner implements Runnable {
		public QueryResult result;
		
		private int queryId;
		private int setId;
		private int iteration;

		public QueryRunner(String query, int iteration, int setId, int queryId) {
			QueryResult result = new QueryResult(query);
			this.iteration = iteration;
			this.queryId = queryId;
			this.setId = setId;
			this.result = result;
		}
		
		public int getQueryId () {
			return queryId;
		}

		public void run() {		
			// Now execute the query
			try {
				int runTime = BenchmarkUser.this.executeQuery(this.result.query, this.iteration, this.setId, this.queryId);
				this.result.runtime = runTime;
			} catch(Exception e) {
				BenchmarkUser.this.owner.printError("Error in query #" + this.queryId + ": " + e.getMessage());
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
