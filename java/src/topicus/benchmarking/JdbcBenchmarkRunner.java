package topicus.benchmarking;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class JdbcBenchmarkRunner extends AbstractBenchmarkRunner {
	protected Connection[] conns = new Connection[NR_OF_QUERIES];
	protected Statement[] statements = new Statement[NR_OF_QUERIES];
	protected long[] startTimes = new long[NR_OF_QUERIES];
	protected boolean[] isCancelled = new boolean[NR_OF_QUERIES];

	protected TimeoutWatcher timeoutWatcher = null;


	@Override
	public void prepareBenchmark() throws PrepareException {
		startTimes[0] = -1;
		startTimes[1] = -1;
		startTimes[2] = -1;
		startTimes[3] = -1;
		
		try {
			this.setupConnections();
		} catch (SQLException e) {
			throw new PrepareException("SQLException: " + e.getMessage());
		}		
		
		// start timeout watcher
		this.timeoutWatcher = new TimeoutWatcher(this);
		this.timeoutWatcher.start();
	}

	@Override
	public void runIteration(int iteration) {
		Iterator<String[]> iter = this.queryList.iterator();
		
		int setNum = 1;
		while(iter.hasNext()) {
			if (this.isFailed) break;
			
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
				try {
					threads.get(t).join();
				} catch (InterruptedException e) {
					// don't care
				}
				
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
		this.timeoutWatcher.interrupt();
				
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
	
	protected  void setupConnections () throws SQLException {
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
						
			if (result.next() == false) {
				owner.printError("Warning: got no results for query:");
				owner.printError(query);
			}
			
			// free resources
			result.close();
			stmt.close();
			
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
	
	public class TimeoutWatcher extends Thread {
		protected JdbcBenchmarkRunner owner;
		
		public TimeoutWatcher (JdbcBenchmarkRunner owner) {
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
			
			try {
				while(true) {
					// wait certain number before checking again
					Thread.sleep(sleep);
					
					// check for timed-out queries
					owner.checkTimeouts();
				}
			} catch (InterruptedException e) {
				// don't care!
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
				int runTime = JdbcBenchmarkRunner.this.executeQuery(this.result.query, this.iteration, this.setId, this.queryId);
				this.result.runtime = runTime;
			} catch(Exception e) {
				JdbcBenchmarkRunner.this.owner.printError("Error in query #" + this.queryId + ": " + e.getMessage());
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