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

import org.apache.commons.cli.CommandLine;

import au.com.bytecode.opencsv.CSVWriter;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.databases.AbstractDatabase;

public abstract class AbstractUnloadTenant extends DatabaseScript {
	protected String tenantDirectory = "";
	protected int tenantId = -1;
	protected Connection conn;
	
	protected String outputDirectory;
	protected String outputFile;
	protected CSVWriter resOut = null;
	
	protected int nodeCount = -1;

	public abstract void _deleteDataFromTable(String tableName, String tenantField) throws SQLException;
	public abstract void _deleteDataFromClosure(int beginKey, int endKey) throws SQLException;
	
	protected abstract int _doDeployData(String fileName, String tableName) throws SQLException;
	
	public AbstractUnloadTenant(String type, AbstractDatabase database) {
		super(type, database);
	}
	
	public void run () throws Exception {	
		printLine("Started-up tenant unloading tool");	
		printLine("Type set to: " + this.type);
		
		this.printLine("Setting up connection");
		this.conn = this._setupConnection();
		this.printLine("Connection setup");
		
		this._setOptions();		
		
		this._checkIfDeployed();
		
		this.printLine("Starting deployment of tenant data for tenant #" + this.tenantId);
		
		this._deployData("adm_data.tbl", "dim_administratie");
		this._deployData("gb_data.tbl", "dim_grootboek");
		this._deployData("kp_data.tbl", "dim_kostenplaats");
		this._deployData("org_data.tbl", "organisatie");
		this._deployData("closure_org_data.tbl", "closure_organisatie");
		this._deployData("fe_data.tbl", "fact_exploitatie");
		
		this.printLine("Finished deployment of tenant data");		
		
		this.conn.close();
		this.printLine("Stopping");
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
	
	protected void _deleteOldData(String tableName) throws SQLException {
		this.printLine("Deleting old tenant data from `" + tableName + "`");
		
		if (tableName.equals("closure_organisatie")) {
			// number of rows in the organisation table
			final int ORG_ROW_COUNT = 988;
			final int ORG_BEGIN_PK = 752;
			
			int beginPk = (this.tenantId - 1) * ORG_ROW_COUNT + ORG_BEGIN_PK;
			int endPk = beginPk + ORG_ROW_COUNT;
			
			this._deleteDataFromClosure(beginPk, endPk);
		} else {
			String tenantField = "";
			
			if (tableName.equals("dim_administratie")) tenantField = "a_tenant";
			if (tableName.equals("dim_grootboek")) tenantField = "gb_tenant";
			if (tableName.equals("dim_kostenplaats")) tenantField = "kp_tenant";
			if (tableName.equals("organisatie")) tenantField = "afnemer";
			if (tableName.equals("fact_exploitatie")) tenantField = "tenant_key";
			
			this._deleteDataFromTable(tableName, tenantField);
		}		
		
		this.printLine("Old data deleted");
	}
	
	protected void _deployData(String tableFile, String tableName) throws SQLException {
		this._deleteOldData(tableName);
		
		this.printLine("Deploying `" + tableFile + "` into `" + tableName + "`");	
		
		long start = System.currentTimeMillis();
		int rows = this._doDeployData(tableFile,  tableName);
		int runTime = (int) (System.currentTimeMillis() - start);
		
		this.printLine("Inserted " + rows + " rows");
		
		this.addResult(tableName,  rows, runTime);		
	}
	
	protected void _checkIfDeployed () throws SQLException, IOException {
		if (this.isTenantDeployed()) {
			this.printLine("Tenant #" + this.tenantId + " is already deployed!");
			
			if (this.cliArgs.hasOption("stop-on-deployed")) {
				printLine("--stop-on-deployed detected, stopping");			
				System.exit(0);
			} else {
				boolean doDeploy = this.confirmBoolean("Are you sure you want to re-deploy? (y/n)");
				if (!doDeploy) {
					printLine("Stopping");
					System.exit(0);
				}				
			}
		}		
	}
		
	protected boolean isTenantDeployed () throws SQLException {
		boolean ret = false;
		
		PreparedStatement q = this.conn.prepareStatement("SELECT a_tenant FROM dim_administratie WHERE a_tenant = ? limit 1");
		q.setInt(1,  this.tenantId);
		
		ResultSet result = q.executeQuery();
		
		if (result.next()) {
			ret = true;
		}
		
		return ret;
	}
	
	protected void _setOptions () throws Exception {
		this.tenantDirectory = this.cliArgs.getOptionValue("tenant-data", "");
		if (this.tenantDirectory.length() == 0) {
			this.printError("Missing tenant directory");
			System.exit(0);
		}
		
		if (this.tenantDirectory.endsWith("/") == false
			&& this.tenantDirectory.endsWith("\\") == false) {
				this.tenantDirectory += "/";
		}
		
		this.printLine("Tenant directory set to: " + this.tenantDirectory);
		
		this.tenantId = Integer.parseInt(this.cliArgs.getOptionValue("tenant-id", "0"));
		if (this.tenantId < 1) {
			this.printError("Invalid tenant ID specified");
			System.exit(0);
		}
		
		this.printLine("Tenant ID set to: " + this.tenantId);
		
		this.printLine("Getting node count");
		this.nodeCount = this.database.getNodeCount(conn);
		this.printLine("Found " + this.nodeCount + " nodes");
		
		// output directory
		this.outputDirectory = cliArgs.getOptionValue("output");
		File outputDir = new File(this.outputDirectory);
		if (outputDir.exists() == false || outputDir.isFile()) {
			throw new Exception("Invalid output directory specified");		
		}
		this.outputDirectory = outputDir.getAbsolutePath() + "/";
		
		this.outputFile = this.outputDirectory + "load-" + this.type +
						  "-" + this.nodeCount + "-nodes" +
						  "-tenant-" + this.tenantId;
		this.setupLogging(this.outputFile + ".log");
		this.outputFile += ".csv";
		
		File currOutputFile = new File(this.outputFile);
		if (currOutputFile.exists()) {
			boolean doOverwrite = this.confirmBoolean("Results file already exists. Overwrite? (y/n): ");
			if (!doOverwrite) {
				printLine("Quitting benchmark tool");
				System.exit(0);
			}
			
			currOutputFile.delete();
			currOutputFile.createNewFile();
		}
		
		this.resOut = new CSVWriter(new FileWriter(this.outputFile, true), '\t', CSVWriter.NO_QUOTE_CHARACTER);
	}

}
