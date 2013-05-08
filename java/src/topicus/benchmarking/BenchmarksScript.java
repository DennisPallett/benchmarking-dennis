package topicus.benchmarking;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.databases.AbstractDatabase;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

public class BenchmarksScript extends DatabaseScript {
	public class OverwriteException extends Exception {}
	public class InvalidNumberOfNodesException extends Exception {}
	public class InvalidNumberOfTenantsException extends Exception {}
	public class InvalidNumberOfUsersException extends Exception {}
	
	public class MissingResultsFileException extends Exception {
		public MissingResultsFileException(String string) {
			super(string);
		}
	}
	
	protected String ID;
	
	protected static final int TOO_SLOW = 5000;
	public static final int QUERY_TIMEOUT = 10000;
	
	protected List<String[]> queryList;
	protected int iterations;
	protected int nodes;
	protected int numberOfUsers;
	
	protected int tenantCount;
	protected int numberOfTenants;
	
	protected int nodeCount;
	
	protected String outputFile = null;
	protected String outputDirectory;
	protected CSVWriter resOut = null;
	
	protected String logFile = null;		
	
	protected List<HashMap<String, String>> serverNodes = new ArrayList<HashMap<String, String>>();
		
	protected List<BenchmarkUser> users;
	
	protected List<int[]> results = new ArrayList<int[]>();
	
	protected String connUrl = "";
	
	protected boolean slowStop = false;
	
	public BenchmarksScript(String type, AbstractDatabase database) {
		super(type, database);
		
		// generate a unique ID for this benchmark
		// the "a" is added to make sure the ID does not start
		// with a number
		this.ID = "a" + UUID.randomUUID().toString();
	}
	
	public String getID () {
		return this.ID;
	}
				
	public void run() throws Exception {		
		printLine("Started-up benchmark tool");
		
		this.setupLogging(cliArgs.getOptionValue("log-file"));
						
		String queriesFile = cliArgs.getOptionValue("queries");		
		this.loadQueries(queriesFile);
		
		printLine("ID of this benchmark: " + this.ID);
		
		// how often should the queries be run
		this.iterations = Integer.parseInt(cliArgs.getOptionValue("iterations", "3"));
		printLine("Number of iterations set to: " + this.iterations);
		
		// how many concurrent users
		this.numberOfUsers = Integer.parseInt(cliArgs.getOptionValue("users", "1"));
		printLine("Number of concurrent users set to: " + this.numberOfUsers);
		
		// how many nodes
		this.nodes = Integer.parseInt(cliArgs.getOptionValue("nodes", "1"));
		printLine("Number of nodes set to: " + this.nodes);
		
		// how many tenants
		this.numberOfTenants = Integer.parseInt(cliArgs.getOptionValue("tenants", "1"));
				
		// load database config
		this._loadDbConfig();
		
		// get information about current system setup
		this._getDatabaseInfo();		
		
		this.outputFile = cliArgs.getOptionValue("results-file", "");
		if (this.outputFile.length() == 0) {
			throw new MissingResultsFileException("You must specify a results filename with --results-file");
		}
			
		File currOutputFile = new File(this.outputFile);
		if (currOutputFile.exists()) {
			printError("Results file already exists");
			if (this.cliArgs.hasOption("overwrite-existing") == false 
				&&
				(this.cliArgs.hasOption("stop-on-overwrite") || confirmBoolean("Overwrite existing file? (y/n)") == false)) {
					printError("Unable to overwrite");
					throw new OverwriteException();
			}
			
			currOutputFile.delete();
			currOutputFile.createNewFile();
		}
		
		printLine("Writing results to: " + this.outputFile);
		
		if (this.tenantCount < this.numberOfUsers) {
			this.printError("Not enough tenants in database for number of concurrent users");
			throw new InvalidNumberOfTenantsException();
		}
		
		if (this.tenantCount != this.numberOfTenants) {
			this.printError("Number of tenants (" + this.tenantCount + ") in database does not " +
					"equal specified number of tenants (" + this.numberOfTenants + ")");
			throw new InvalidNumberOfTenantsException();
		}
		
		if (this.nodeCount != this.nodes) {
			this.printError("Number of online nodes (" + this.nodeCount + ") does not equal specified number of nodes (" + this.nodes + ")");
			throw new InvalidNumberOfNodesException();
		}	
		
		this.resOut = new CSVWriter(new FileWriter(this.outputFile, true), '\t', CSVWriter.NO_QUOTE_CHARACTER);
		
		// setup benchmark users
		this._setupUsers();
		
		// short pause to catch up
		// after creating all users
		Thread.sleep(1000);
		
		if (!this.cliArgs.hasOption("start")) {
			if (this.confirmBoolean("Run benchmarks?") == false) {
				System.exit(ExitCodes.NO_START);
			}
		}
		
		// start up separate slowness checker thread
		SlowCheckThread slowChecker = new SlowCheckThread(this);
		slowChecker.start();
		
		this.runBenchmarks();	
		
		// stop slowchecker (if still running)
		slowChecker.interrupt();
				
		this.printSummary();		
		
		printLine("Results saved to: " + this.outputFile);	
		printLine("Quitting tool");	
		
		this.resOut.close();
		
		if (this.logOut != null) {
			this.logOut.close();
		}
		
		if (this.slowStop) {
			throw new TooSlowException("Query execution time too slow!");
		}		
	}
	
	public void addResult(int userId, int iteration, int set, int query1_time, int query2_time, int query3_time, int query4_time, int setTime) {
		// directly write to file
		this.resOut.writeNext(new String[] {
			String.valueOf(userId),
			String.valueOf(iteration),
			String.valueOf(set),
			String.valueOf(query1_time),
			String.valueOf(query2_time),
			String.valueOf(query3_time),
			String.valueOf(query4_time),
			String.valueOf(setTime)
		});
		
		try {
			this.resOut.flush();
		} catch (IOException e) {
			this.printError("Unable to write to results file!");
			printLine("Quitting tool");	
			System.exit(ExitCodes.UNABLE_TO_WRITE_RESULTS);
		}
		
		// also add to internal memory for summary
		this.results.add(new int[] {
			userId,
			iteration,
			set,
			query1_time,
			query2_time,
			query3_time,
			query4_time,
			setTime
		});
	}
	
	public void doSlowCheck () {
		HashMap<String, Float> stats = this.calculateStats();
		
		if (stats.get("queryCount") == 0 || stats.get("queryAvg") > TOO_SLOW) {
			this.printError("Queries are too slow, current average: " + stats.get("queryAvg"));
			this.slowStop();
		} else {
			printLine("Passed slowcheck, current average: " + stats.get("queryAvg") + " over " + stats.get("queryCount") + " queries");
		}
	}
	
	protected synchronized void slowStop () {
		this.slowStop = true;
		
		for (BenchmarkUser user : this.users) {
			user.interrupt();
		}
	}
	
	public HashMap<String, Float> calculateStats () {
		HashMap<String, Float> stats = new HashMap<String, Float>();
		
		// calculate query average
		int queryMax = Integer.MIN_VALUE;
		int queryMin = Integer.MAX_VALUE;
		int setMax = Integer.MIN_VALUE;
		int setMin = Integer.MAX_VALUE;
		
		int queryTotal = 0;
		int queryCount = 0;
		int timeoutCount = 0;
		
		int setTotal = 0;
		int setCount = 0;
		
		for (int i=0; i < this.results.size(); i++) {
			int[] result = results.get(i);	
			int setTime = result[7];
			
			boolean hasFailedQueries = false;
			for (int j=3;j < 7; j++) {
				if (result[j] == QUERY_TIMEOUT) {
					hasFailedQueries = true;
					timeoutCount++;
				} else if (result[j] > 0) {
					int queryTime = result[j];
					
					queryTotal += queryTime;
					queryCount++;
					
					if (queryTime < queryMin) queryMin = queryTime;
					if (queryTime > queryMax) queryMax = queryTime;
				}
			}
			
			if (hasFailedQueries == false) {
				setTotal += setTime;
				if (setTime < setMin) setMin = setTime;
				if (setTime > setMax) setMax = setTime;
				setCount++;
			}
			
		}
		
		float queryAvg = 0;
		if (queryCount > 0) {
			queryAvg = queryTotal / queryCount;
		}
		
		float setAvg = 0;
		if (setCount > 0) {
			setAvg = setTotal / setCount;
		}
				
		stats.put("timeoutCount", Float.valueOf(timeoutCount));
		stats.put("queryCount",  Float.valueOf(queryCount));
		stats.put("queryMax", Float.valueOf(queryMax));
		stats.put("queryAvg", Float.valueOf(queryAvg));
		stats.put("queryMin", Float.valueOf(queryMin));
		
		stats.put("setCount",  Float.valueOf(setCount));
		stats.put("setMax", Float.valueOf(setMax));
		stats.put("setAvg", Float.valueOf(setAvg));
		stats.put("setMin", Float.valueOf(setMin));
		
		return stats;
	}
	
	public void printSummary () {
		HashMap<String, Float> stats = this.calculateStats();
		
		DecimalFormat df = new DecimalFormat("#");
		
		this.printLine("---");
		this.printLine("Results summary:");
		this.printLine("----------------");
		this.printLine("Number of queries: " + df.format(stats.get("queryCount")));
		this.printLine("Min query time: " + stats.get("queryMin") + " ms");
		this.printLine("Avg query time: " + stats.get("queryAvg") + " ms");
		this.printLine("Max query time: " + stats.get("queryMax") + " ms");
		this.printLine("Number of queries timed-out: " + df.format(stats.get("timeoutCount")));
		this.printLine("----------------");	
		this.printLine("Number of sets: " + stats.get("setCount"));
		this.printLine("Min set time: " + stats.get("setMin") + " ms");
		this.printLine("Avg set time: " + stats.get("setAvg") + " ms");
		this.printLine("Max set time: " + stats.get("setMax") + " ms");
	}	
	
	protected void runBenchmarks () throws InterruptedException {
		this.printLine("Running benchmarks");
		
		synchronized(this) {
			this.notifyAll();
		}
		
		// wait until all users have finished running benchmarks
		for(int i=0; i < this.users.size(); i++) {
			this.users.get(i).join();
		}
	}
	
	protected void _getDatabaseInfo () throws SQLException {				
		// setup connection
		Connection conn = this.database.setupConnection("node1");
		
		this.tenantCount = this.getTenantCount(conn);
		
		this.printLine("Retrieving node count");
		this.nodeCount = this.database.getNodeCount(conn);
		this.printLine("Found " + this.nodeCount + " nodes for database");
		
		conn.close();
	}
	
	protected int getTenantCount (Connection conn) throws SQLException {
		this.printLine("Retrieving tenant count");

		// execute COUNT query
		Statement stmt = conn.createStatement();
		ResultSet result = stmt.executeQuery("SELECT COUNT(DISTINCT a_tenant) AS tenant_count FROM dim_administratie WHERE a_tenant != 0;");
		
		// retrieve tenant count
		result.next();
		int tenantCount = result.getInt("tenant_count");
		
		// close everything
		result.close();		
		stmt.close();
		
		this.printLine("Found " + tenantCount + " tenants in database");		
		return tenantCount;
	}
	
	protected void _setupUsers () throws Exception {
		this.printLine("Setting up benchmark users");
		
		this.users = new ArrayList<BenchmarkUser>();
		for(int i=1; i <= this.numberOfUsers; i++) {
			BenchmarkUser user = new BenchmarkUser(
				this,
				i,
				this.queryList,
				this.iterations,
				this.nodes,
				this.numberOfTenants,
				this.database
			);
			
			user.start();
			this.users.add(user);
		}
		
		// wait for all users to finish
		boolean allReady = false;
		while (!allReady) {
			allReady = true;
			for(int i=0; i < this.users.size(); i++) {
				BenchmarkUser user = this.users.get(i);
				
				if (user.isFailed()) {
					throw user.getFailException();
				}
				
				if (!user.isReady()) {
					allReady = false;
					break;
				}
			}
			
			if (!allReady) {
				Thread.sleep(1000);
			}
		}		
		
		this.printLine("All users setup");	
	}
	
	protected void loadQueries(String queriesFile) throws Exception {
		printLine("Loading queries from CSV file: " + queriesFile);
		
		CSVReader reader = new CSVReader(new FileReader(queriesFile), ',', '"');
		this.queryList = reader.readAll();
		
		// validate query list
		Iterator<String[]> iter = this.queryList.iterator();
		while(iter.hasNext()) {
			String[] queries = iter.next();
			
			// verify set has 4 queries
			if (queries.length != 4) {
				this.printError("Invalid query CSV file, sub-set does not have 4 queries!");
				System.exit(ExitCodes.INVALID_QUERIES);
			}
		}
		
		// all OK! queries loaded
		printLine("Queries loaded, found " + this.queryList.size() + " sets");
	}
	
	protected class SlowCheckThread extends Thread {
		BenchmarksScript owner;
		public SlowCheckThread (BenchmarksScript owner) {
			this.owner = owner;
		}
		
		public void run () {
			// wait to do slow check
			try {
				Thread.sleep(60*1000);
				
				// check for slowness
				owner.doSlowCheck();	
			} catch (InterruptedException e) {

			}					
		}
	}
		
	
	public class TooSlowException extends Exception {
		public TooSlowException(String string) {
			super(string);
		}
	}

}
