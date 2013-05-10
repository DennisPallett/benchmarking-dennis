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
	protected AbstractBenchmarkRunner runner;
	
	protected Random random;	
	
	protected int userId;		
	
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
		
	public BenchmarkUser (BenchmarksScript owner, int userId, List<String[]> queryList, 
			int iterations, int nodes, int numberOfTenants, AbstractDatabase database) throws SQLException {
		this.owner = owner;
		this.userId = userId;
		this.queryList = queryList;
		this.iterations = iterations;
		this.nodes = nodes;
		this.numberOfTenants = numberOfTenants;
		this.database = database;
		
		this.runner = database.createBenchmarkRunner();
		runner.setDatabase(database);
		runner.setUserId(userId);
		runner.setOwner(owner);
		runner.setNodes(nodes);
		runner.setQueryList(queryList);		
				
		
		
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
			this.runner.prepareBenchmark();
			
			this.owner.printLine("User #" + this.userId + " ready and waiting");
			this.isReady = true;
			
			synchronized(this.owner) {
				try {
					this.owner.wait();
				} catch(InterruptedException e){
					// don't care, we can continue!
				}
			}
			
			try {
				this.runBenchmarks();
			} catch (InterruptedException e) {
				this.isFailed = true;
				this.failException = e;
				this.owner.printError("User #" + this.userId + " failed during benchmarking");
			}
		} catch (Exception e) {
			this.isFailed = true;
			this.failException = e;
			this.owner.printError("User #" + this.userId + " failed");
		}		
		
		// finish benchmark
		owner.printLine("Finishing benchmark for user #" + userId);
		runner.finishBenchmark();		
	}
	
	protected void runBenchmarks () throws InterruptedException {
		this.owner.printLine("User #" + this.userId + " starting benchmarks");		
		
		for(int i=1; i <= this.iterations; i++) {
			this.owner.printLine("User #" + this.userId + " running iteration " + i);
			this.runner.runIteration(i);
			this.owner.printLine("User #" + this.userId + " finished iteration " + i);
		}
	}	

}
