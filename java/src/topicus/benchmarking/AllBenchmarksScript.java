package topicus.benchmarking;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.databases.AbstractDatabase;
import topicus.loadtenant.LoadTenantScript;
import topicus.loadtenant.LoadTenantScript.AlreadyDeployedException;
import topicus.loadtenant.RunLoadTenant;

public class AllBenchmarksScript extends DatabaseScript {
	public class CancelledException extends Exception {};	
	public class InvalidQueriesFileException extends Exception {};
	
	public class MissingAwsCredentialsException extends Exception {
		public MissingAwsCredentialsException(String string) {
			super(string);
		}
	}	
	public class InvalidBucketException extends Exception {
		public InvalidBucketException(String string) {
			super(string);
		}
	}	
	
	protected int nodes;
	protected int nodeCount;
	
	protected int iterations;
	
	protected Connection conn;
	
	protected String outputDirectory;
	protected String tenantDirectory;
	
	protected String bucketName;
	protected String awsCredentialsFile;
	
	protected AmazonS3 s3;
	
	protected String queriesFile;		
	
	protected static final int[] users = {1, 5, 10, 20, 50, 100};
	protected static final int[] tenants = {1, 5, 10, 20, 50, 100};
	
	protected ArrayList<Integer> deployedTenants = new ArrayList<Integer>();
	
	public AllBenchmarksScript(String type, AbstractDatabase database) {
		super(type, database);
	}

	public void run () throws Exception {
		printLine("Started-up benchmark tool for a complete set of benchmarks");	
		
		
		
		this._setOptions();
		
		// confirm running of benchmarks
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Do you want to start running benchmarks? (y/n)")) {
				printLine("Stopping");
				System.exit(ExitCodes.NO_START);
			}
		}
		
		for(int tenant : tenants) {
			this._deployTenants(tenant);
			
			for(int user : users) {
				if (tenant >= user) {					
					this._runBenchmark(tenant, user);
				}
			}
		}		
		
		printLine("Successfully finished!");
		System.exit(0);
	}
	
	protected void _setOptions () throws Exception {
		// how many nodes
		this.nodes = Integer.parseInt(cliArgs.getOptionValue("nodes", "1"));
		printLine("Number of nodes set to: " + this.nodes);
		
		// how often should the queries be run
		this.iterations = Integer.parseInt(cliArgs.getOptionValue("iterations", "5"));
		printLine("Number of iterations set to: " + this.iterations);
		
		this.queriesFile = cliArgs.getOptionValue("queries");
		File testFile = new File(this.queriesFile);
		if (testFile.exists() == false) {
			throw new InvalidQueriesFileException();
		}
		printLine("Queries file set to: " + this.queriesFile);				
		
		this.printLine("Setting up connection with database");
		this.conn = this._setupConnection();
		this.printLine("Connection setup");
		
		// check nodes with nodeCount
		this.nodeCount = this.database.getNodeCount(conn);
		if (nodes != this.nodeCount) {
			this.printError("Number of active nodes (" + this.nodeCount + ") does not equal number of set nodes (" + nodes + ")");
			System.exit(ExitCodes.INVALID_NUMBER_OF_NODES);
		}
		
		this.conn.close();
		
		// output directory
		this.outputDirectory = cliArgs.getOptionValue("output", "");
		File outputDir = new File(this.outputDirectory);
		if (outputDir.exists() == false || outputDir.isFile()) {
			throw new Exception("Invalid output directory specified");		
		}
		this.outputDirectory = outputDir.getAbsolutePath() + "/";
		
		this.tenantDirectory = this.cliArgs.getOptionValue("tenant-data", "");
		if (this.tenantDirectory.length() == 0) {
			this.printError("Missing tenant directory");
			System.exit(0);
		}
		
		if (this.tenantDirectory.endsWith("/") == false
			&& this.tenantDirectory.endsWith("\\") == false) {
				this.tenantDirectory += "/";
		}
		
		this.bucketName = cliArgs.getOptionValue("bucket", "");
		if (this.bucketName.length() == 0) {
			this.printLine("No S3 bucket specified");
			if (confirmBoolean("Are you sure you don't want to specify a S3 bucket to upload results to? (y/n)") == false) {
				this.printError("Stopping, please restart with S3 bucket specification (--bucket [name])");
				throw new Exception();
			}
		}
		
		// check for AWS credentials
		if (this.bucketName.length() > 0) {
			this.awsCredentialsFile = cliArgs.getOptionValue("aws-credentials", "");
			printLine(this.awsCredentialsFile);
			File awsFile = new File(this.awsCredentialsFile);
			if (awsFile.exists() == false) {
				throw new MissingAwsCredentialsException("Missing AWS credentials file");
			}
			
			// setup S3 client
			s3 = new AmazonS3Client(new PropertiesCredentials(awsFile));
			s3.setRegion(Region.getRegion(Regions.EU_WEST_1));
						
			// check if bucket exists
			if (!s3.doesBucketExist(this.bucketName)) {
				throw new InvalidBucketException("Specified S3 bucket `" + this.bucketName + "` does not exist");
			}
		}
	}
	
	protected void _deployTenants(int numberOfTenants) throws Exception {
		if (!this.confirmBoolean("Ready to deploy tenants 1 through " + numberOfTenants + ". Continue? (y/n)")) {
			this.printError("Stopping");
			throw new CancelledException();
		}
		
		for(int tenantId=1; tenantId <= numberOfTenants; tenantId++) {
			this._deployTenant(tenantId);
		}
	}
	
	protected void _runBenchmark(int numberOfTenants, int numberOfUsers) throws Exception {
		this._runBenchmark(numberOfTenants, numberOfUsers, false);
	}
	
	protected void _runBenchmark(int numberOfTenants, int numberOfUsers, boolean doOverwrite) throws Exception {
		boolean doBenchmark = this.confirmBoolean("Ready to run benchmark: " + this.nodes + " nodes, " 
				+ numberOfTenants + " tenants, " + numberOfUsers + " users. Continue? (y/n)");
		
		if (!doBenchmark) {
			this.printError("Stopping");
			throw new CancelledException();
		}
		
		String fileName = "benchmark";
		fileName += "-" + this.type;
		fileName += "-" + numberOfUsers + "-users";
		fileName += "-" + this.nodes + "-nodes";
		fileName += "-" + numberOfTenants + "-tenants";
		
		ArrayList<String> args = new ArrayList<String>();
		
		args.add("--type");
		args.add(this.type);
		
		args.add("--queries");
		args.add(this.queriesFile);
		
		args.add("--iterations");
		args.add(String.valueOf(this.iterations));
		
		args.add("--nodes");
		args.add(String.valueOf(this.nodes));
		
		args.add("--tenants");
		args.add(String.valueOf(numberOfTenants));
		
		args.add("--users");
		args.add(String.valueOf(numberOfUsers));
		
		args.add("--results-file");
		args.add(this.outputDirectory + fileName + ".csv");
		
		args.add("--log-file");
		args.add(this.outputDirectory + fileName + ".log");
		
		if (doOverwrite) {
			args.add("--overwrite-existing");
		} else {
			args.add("--stop-on-overwrite");
		}
		
		args.add("--start"); 
		args.add("y");
		
		try {
			RunBenchmarks.main( args.toArray(new String[args.size()]) );
			
			// upload results to S3
			printLine("Uploading results to S3");
			s3.putObject(this.bucketName, fileName, new File(this.outputDirectory + fileName + ".csv"));
			printLine("Upload finished");
			
			
		// can't overwrite existing results file
		// likely means we already ran this benchmark
		} catch (BenchmarksScript.OverwriteException e) {
			// no problemo!
			
		// incorrect number of nodes behind database?
		} catch (BenchmarksScript.InvalidNumberOfNodesException e) {
			if (this.confirmBoolean("Run benchmark again? (y/n)")) {
				this._runBenchmark(numberOfTenants, numberOfUsers, true);
			} else {
				printError("Stopping");
				throw new CancelledException();
			}
			
		// incorrect number of tenants in database?
		// shouldn't be possible but could happen!
		} catch (BenchmarksScript.InvalidNumberOfTenantsException e) {
			if (this.confirmBoolean("Try to re-deploy tenants and run benchmark again? (y/n)")) {
				this._deployTenants(numberOfTenants);
				this._runBenchmark(numberOfTenants, numberOfUsers, true);
			} else {
				printError("Stopping");
				throw new CancelledException();
			}		
			
		// some other exception?
		} catch (Exception e) {
			this.printError("Failed to run benchmarks: " + e.getMessage());
			if (this.confirmBoolean("Try to run again? (y/n)")) {
				this._runBenchmark(numberOfTenants, numberOfUsers);
			} else {
				printError("Stopping");
				throw new CancelledException();
			}
		}
		
		printLine("Benchmark finished");
	}
	
	protected void _deployTenant(int tenantId) throws Exception {
		this._deployTenant(tenantId, false);
	}
	
	protected void _deployTenant(int tenantId, boolean doOverwrite) throws Exception {
		this.printLine("Deploying tenant #" + tenantId);
		if (this.deployedTenants.contains(tenantId)) {
			this.printLine("Tenant #" + tenantId + " is already deployed");
			return;
		}
		
		String fileName = "load";
		fileName += "-" + this.type;
		fileName += "-" + this.nodes + "-nodes";
		fileName += "-tenant-" + tenantId;
				
		ArrayList<String> args = new ArrayList<String>();
		args.add("--type"); 
		args.add(this.type);
		
		args.add("--tenant-data"); 
		args.add(this.tenantDirectory + tenantId + "/");
		
		args.add("--tenant-id"); 
		args.add(String.valueOf(tenantId));
		
		args.add("--results-file");
		args.add(this.outputDirectory + fileName + ".csv");
		
		args.add("--log-file");
		args.add(this.outputDirectory + fileName + ".log");
		
		if (!doOverwrite) {
			args.add("--stop-on-overwrite");
		}
		
		args.add("--stop-on-deployed"); 
		
		args.add("--start"); 
		args.add("y");
		
		
		try {
			RunLoadTenant.main( args.toArray(new String[args.size()]) );
			
			// upload results to S3
			printLine("Uploading results to S3");
			s3.putObject(this.bucketName, fileName, new File(this.outputDirectory + fileName + ".csv"));
			printLine("Upload finished");
		} catch (LoadTenantScript.OverwriteException e) {
			// no problemo!
		} catch (AlreadyDeployedException e) {
			// no problemo!
		} catch (Exception e) {
			this.printError("Failed to deploy tenant: " + e.getMessage());
			if (this.confirmBoolean("Try to deploy again? (y/n)")) {
				this._deployTenant(tenantId);
			} else {
				printError("Stopping");
				throw new CancelledException();
			}
		}
		
		
		this.printLine("Tenant #" + tenantId + " deployed");
		this.deployedTenants.add(tenantId);
	}
	
	
	
}
