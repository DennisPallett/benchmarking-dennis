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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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

public class Benchmarks extends DatabaseScript {
	protected static final int TOO_SLOW = 5000;
	
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
	
	protected int slowCount = 0;
	
	public Benchmarks(String type, AbstractDatabase database) {
		super(type, database);
	}
				
	public void run() throws Exception {
		printLine("Started-up benchmark tool");		
						
		String queriesFile = cliArgs.getOptionValue("queries");		
		this.loadQueries(queriesFile);
		
		// how often should the queries be run
		this.iterations = Integer.parseInt(cliArgs.getOptionValue("iterations", "5"));
		printLine("Number of iterations set to: " + this.iterations);
		
		// how many concurrent users
		this.numberOfUsers = Integer.parseInt(cliArgs.getOptionValue("users", "1"));
		printLine("Number of concurrent users set to: " + this.numberOfUsers);
		
		// how many nodes
		this.nodes = Integer.parseInt(cliArgs.getOptionValue("nodes", "1"));
		printLine("Number of nodes set to: " + this.nodes);
		
		// how many tenants
		this.numberOfTenants = Integer.parseInt(cliArgs.getOptionValue("tenants", "1"));
			
		// output directory
		this.outputDirectory = cliArgs.getOptionValue("output");
		File outputDir = new File(this.outputDirectory);
		if (outputDir.exists() == false || outputDir.isFile()) {
			throw new Exception("Invalid output directory specified");		
		}
		this.outputDirectory = outputDir.getAbsolutePath() + "/";
		
		printLine("Writing results and log to: " + this.outputDirectory);
				
		// load database config
		this._loadDbConfig();
		
		// get information about current system setup
		this._getDatabaseInfo();
		
		if (this.tenantCount < this.numberOfUsers) {
			this.printError("Not enough tenants in database for number of concurrent users");
			System.exit(ExitCodes.INVALID_NUMBER_OF_USERS);
		}
		
		if (this.tenantCount != this.numberOfTenants) {
			this.printError("Number of tenants (" + this.tenantCount + ") in database does not " +
					"equal specified number of tenants (" + this.numberOfTenants + ")");
			System.exit(ExitCodes.INVALID_NUMBER_OF_TENANTS);
		}
		
		if (this.nodeCount != this.nodes) {
			this.printError("Number of online nodes (" + this.nodeCount + ") does not equal specified number of nodes (" + this.nodes + ")");
			System.exit(ExitCodes.INVALID_NUMBER_OF_NODES);
		}
		
		// setup benchmark users
		this._setupUsers();
		
		// create output file name and log file name
		this.outputFile = this.outputDirectory + "benchmark-";
		this.outputFile+= this.type + "-";
		this.outputFile+= this.numberOfUsers + "-users-";
		this.outputFile+= this.nodes + "-nodes-";
		this.outputFile+= this.tenantCount + "-tenants-";
		this.outputFile+= this.iterations + "-iterations";		
		
		this.logFile = this.outputFile + ".log";
		this.outputFile += ".csv";		
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		File currOutputFile = new File(this.outputFile);
		if (currOutputFile.exists()) {
			printLine("Results file already exists. Overwrite? (y/n): ", false, false);
			String overwrite = br.readLine().toLowerCase();
			printLine("");
			if (!overwrite.equals("y") && !overwrite.equals("yes")) {
				printLine("Quitting benchmark tool");
				System.exit(ExitCodes.NO_OVERWRITE);
			}
			
			currOutputFile.delete();
			currOutputFile.createNewFile();
		}
		
		this.resOut = new CSVWriter(new FileWriter(this.outputFile, true), '\t', CSVWriter.NO_QUOTE_CHARACTER);
		this.setupLogging(this.logFile);	
		
		if (!this.cliArgs.hasOption("start")) {
			if (this.confirmBoolean("Run benchmarks?") == false) {
				System.exit(ExitCodes.NO_START);
			}
		}
				
		this.runBenchmarks();
		this.printSummary();
		
		printLine("Results saved to: " + this.outputFile);	
		printLine("Quitting tool");	
		
		this.resOut.close();
		this.logOut.close();
		System.exit(0);
	}
	
	public void addResult(int userId, int iteration, int set, int query1_time, int query2_time, int query3_time, int query4_time, int set_time) {
		// directly write to file
		this.resOut.writeNext(new String[] {
			String.valueOf(userId),
			String.valueOf(iteration),
			String.valueOf(set),
			String.valueOf(query1_time),
			String.valueOf(query2_time),
			String.valueOf(query3_time),
			String.valueOf(query4_time),
			String.valueOf(set_time)
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
			set_time				
		});
		
		// too slow trip wire
		if (query1_time >= TOO_SLOW) this.slowCount++;
		if (query2_time >= TOO_SLOW) this.slowCount++;
		if (query3_time >= TOO_SLOW) this.slowCount++;
		if (query4_time >= TOO_SLOW) this.slowCount++;
		
		// check number of slow queries	
		if (this.slowCount >= ((this.results.size()*4) * 0.1) 				// bigger than 10% of totally executed queries
			&& this.slowCount >= (this.queryList.size()*this.numberOfUsers)	// bigger than at least 10 queries of each user
			) {
				this.printError("Too many slow queries detected!");
				this.printLine("Stopping");
				System.exit(ExitCodes.TOO_SLOW);
		}
		
		
		
		if ( (this.results.size() % (20 * this.numberOfUsers)) == 0) {
			this.printSummary();
		}
	}
	
	public void printSummary () {
		// calculate query average
		int queryMax = Integer.MIN_VALUE;
		int queryMin = Integer.MAX_VALUE;
		int setMax = Integer.MIN_VALUE;
		int setMin = Integer.MAX_VALUE;
		
		int queryTotal = 0;
		int queryCount = 0;
		
		int setTotal = 0;
		int setCount = 0;
		
		for (int i=0; i < this.results.size(); i++) {
			int[] result = results.get(i);	
			int setTime = result[7];
			
			setTotal += setTime;
			if (setTime < setMin) setMin = setTime;
			if (setTime > setMax) setMax = setTime;
			setCount++;
						
			for (int j=3;j < 7; j++) {
				if (result[j] > 0) {
					int queryTime = result[j];
					
					queryTotal += queryTime;
					queryCount++;
					
					if (queryTime < queryMin) queryMin = queryTime;
					if (queryTime > queryMax) queryMax = queryTime;
				}
			}
		}
		
		float queryAvg = queryTotal / queryCount;
		float setAvg = setTotal / setCount;
		
		this.printLine("---");
		this.printLine("Results summary:");
		this.printLine("----------------");
		this.printLine("Number of queries: " + queryCount);
		this.printLine("Min query time: " + queryMin + " ms");
		this.printLine("Avg query time: " + queryAvg + " ms");
		this.printLine("Max query time: " + queryMax + " ms");
		this.printLine("----------------");
		this.printLine("Number of sets: " + setCount);
		this.printLine("Min set time: " + setMin + " ms");
		this.printLine("Avg set time: " + setAvg + " ms");
		this.printLine("Max set time: " + setMax + " ms");
		this.printLine("----------------");	
		this.printLine("Slow count: " + slowCount);
		this.printLine("----------------");	
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
		// setup connection strings
		String url = this.database.getJdbcUrl() + "node1:" + this.dbPort + "/" + this.dbName;
		this.printLine("Connection string: " + url);
					
		// setup connection
		Connection conn = DriverManager.getConnection(url, this.dbUser, this.dbPassword);
		
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
				this.nodes
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
		
	


}
