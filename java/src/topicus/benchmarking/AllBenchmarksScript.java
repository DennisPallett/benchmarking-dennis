package topicus.benchmarking;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.databases.AbstractDatabase;
import topicus.loadtenant.LoadTenantScript;
import topicus.loadtenant.LoadTenantScript.AlreadyDeployedException;
import topicus.loadtenant.RunLoadTenant;

public class AllBenchmarksScript extends DatabaseScript {
	protected int nodes;
	protected int nodeCount;
	
	protected Connection conn;
	
	protected String outputDirectory;
	protected String tenantDirectory;
	
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
	}
	
	protected void _deployTenants(int numberOfTenants) throws IOException {
		if (!this.confirmBoolean("Ready to deploy tenants 1 through " + numberOfTenants + ". Continue? (y/n)")) {
			this.printError("Stopping");
			System.exit(ExitCodes.CANCELLED);
		}
		
		for(int tenantId=1; tenantId <= numberOfTenants; tenantId++) {
			this._deployTenant(tenantId);
		}
	}
	
	protected void _runBenchmark(int numberOfTenants, int numberOfUsers) {
		this.printLine("Starting benchmark: " + this.nodes + " nodes, " + numberOfTenants + " tenants, " + numberOfUsers + " users");
	}
	
	protected void _deployTenant(int tenantId) throws IOException {
		this._deployTenant(tenantId, false);
	}
	
	protected void _deployTenant(int tenantId, boolean doOverwrite) throws IOException {
		this.printLine("Deploying tenant #" + tenantId);
		if (this.deployedTenants.contains(tenantId)) {
			this.printLine("Tenant #" + tenantId + " is already deployed");
			return;
		}
		
		ArrayList<String> args = new ArrayList<String>();
		args.add("--type"); 
		args.add(this.type);
		
		args.add("--tenant-data"); 
		args.add(this.tenantDirectory + tenantId + "/");
		
		args.add("--tenant-id"); 
		args.add(String.valueOf(tenantId));
		
		args.add("--output");
		args.add(this.outputDirectory);
		
		if (!doOverwrite) {
			args.add("--stop-on-overwrite");
		}
		
		args.add("--stop-on-deployed"); 
		
		args.add("--start"); 
		args.add("y");
		
		
		try {
			RunLoadTenant.main( args.toArray(new String[args.size()]) );
		} catch (LoadTenantScript.OverwriteException e) {
			// no problemo!
		} catch (AlreadyDeployedException e) {
			// no problemo!
		} catch (Exception e) {
			this.printError("Failed to deploy tenant: " + e.getMessage());
			if (this.confirmBoolean("Try to deploy again? (y/n)")) {
				this._deployTenant(tenantId);
			} else {
				this.printError("Stopping");
				System.exit(ExitCodes.CANCELLED);
			}
		}
		
		
		this.printLine("Tenant #" + tenantId + " deployed");
		this.deployedTenants.add(tenantId);
	}
	
}
