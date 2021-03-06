package topicus.loadtenant;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Observable;

import org.apache.commons.cli.CommandLine;

import au.com.bytecode.opencsv.CSVWriter;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.benchmarking.BenchmarksScript.MissingResultsFileException;
import topicus.databases.AbstractDatabase;

public class LoadTenantScript extends AbstractTenantScript {
	public class OverwriteException extends Exception {}	
	public class AlreadyDeployedException extends Exception { }
	
	protected String tenantDirectory = "";	
		
	protected String outputDirectory;
	protected String outputFile;
	protected CSVWriter resOut = null;
	
	protected int nodeCount = -1;

	public LoadTenantScript(String type, AbstractDatabase database, ManageTenant manageTenant) {
		super(type, database, manageTenant);
	}
	
	public void run () throws Exception {	
		printLine("Started-up tenant data loading tool");	
		
		this.setupLogging(cliArgs.getOptionValue("log-file"));
		
		printLine("Type set to: " + this.type);
		
		this.printLine("Setting up connection");
		this.conn = this._setupConnection();
		this.manageTenant.setConnection(this.conn);
		this.printLine("Connection setup");
		
		this._setOptions();		
		
		this._checkIfDeployed();
		
		this.printLine("Starting deployment of tenant data for tenant #" + this.tenantId);
		
		this.database.prepareLoadTenant(this);
		
		// loop through tables and deploy
		for(Map.Entry<String, String> entry : this.tables.entrySet()) {
			String tableName = entry.getKey();
			String fileName = entry.getValue();
			
			this._deployData(fileName, tableName);
		}		
		
		this.database.finishLoadTenant(this);
		
		this.printLine("Finished deployment of tenant data");		
		
		this.conn.close();
		this.database.close();
		this.printLine("Stopping");
	}
	
	protected void _checkIfDeployed () throws SQLException, IOException, AlreadyDeployedException {
		if (this.isTenantDeployed()) {
			this.printError("Tenant #" + this.tenantId + " is already deployed!");
			
			if (this.cliArgs.hasOption("stop-on-deployed")) {
				throw new AlreadyDeployedException();
			} else {
				boolean doDeploy = this.confirmBoolean("Are you sure you want to re-deploy? (y/n)");
				if (!doDeploy) {
					printLine("Stopping");
					System.exit(0);
				}				
			}
		}		
	}
	
	protected void addResult(String table, int numberOfRows, int time) {
		this.resOut.writeNext(new String[] {
			table,
			String.valueOf(numberOfRows),
			String.valueOf(time)				
		});
		
		try {
			this.resOut.flush();
		} catch (IOException e) {
			this.printError("Unable to write to results file!");
			printLine("Quitting tool");	
			System.exit(0);
		}
	}
	
	protected void _deployData(String tableFile, String tableName) throws SQLException {
		this._deployData(this.database,  this.conn, tableFile, tableName);
	}
		
	protected void _deployData(AbstractDatabase database, Connection conn, String tableFile, String tableName) throws SQLException {
		this._deleteOldData(conn, tableName);
		
		this.printLine("Deploying `" + tableFile + "` into `" + tableName + "`");	
		
		int[] ret = database.deployData(conn, this.tenantDirectory + tableFile,  tableName);
		
		int runTime = ret[0];
		int rows = ret[1];
		
		this.printLine("Inserted " + rows + " rows");
		
		this.addResult(tableName, rows, runTime);		
	}
		
	protected void _setOptions () throws Exception {
		super._setOptions();
		
		this.tenantDirectory = this.cliArgs.getOptionValue("tenant-data", "");
		if (this.tenantDirectory.length() == 0) {
			this.printError("Missing tenant directory");
			System.exit(0);
		}
		
		if (this.tenantDirectory.endsWith("/") == false
			&& this.tenantDirectory.endsWith("\\") == false) {
				this.tenantDirectory += "/";
		}
		
		this.tenantDirectory += this.tenantId + "/";
		
		this.printLine("Tenant directory set to: " + this.tenantDirectory);
				
		this.printLine("Getting node count");
		this.nodeCount = this.database.getNodeCount(conn);
		this.printLine("Found " + this.nodeCount + " nodes");
		
		this.outputFile = cliArgs.getOptionValue("results-file", "");
		if (this.outputFile.length() == 0) {
			throw new MissingResultsFileException("You must specify a results filename with --results-file");
		}
			
		File currOutputFile = new File(this.outputFile);
		if (currOutputFile.exists()) {
			this.printError("Results file already exists!");
			if (cliArgs.hasOption("overwrite-existing") == false) {
				if (this.cliArgs.hasOption("stop-on-overwrite") || this.confirmBoolean("Overwrite existing file? (y/n): ") == false) {
					throw new OverwriteException();
				}
			}
			
			printLine("Overwriting existing results");
			
			currOutputFile.delete();
			currOutputFile.createNewFile();
		}
		
		this.resOut = new CSVWriter(new FileWriter(this.outputFile, true), '\t', CSVWriter.NO_QUOTE_CHARACTER);
	}
	
	public class MissingResultsFileException extends Exception {
		public MissingResultsFileException(String string) {
			super(string);
		}
	}

	
	
}
