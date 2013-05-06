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
import java.util.Set;

import org.apache.commons.cli.CommandLine;

import au.com.bytecode.opencsv.CSVWriter;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.databases.AbstractDatabase;

public class UnloadTenantScript extends AbstractTenantScript {
	protected String tenantDirectory = "";
	
		
	protected String outputDirectory;
	protected String outputFile;
	protected CSVWriter resOut = null;
	
	protected int nodeCount = -1;

	public UnloadTenantScript(String type, AbstractDatabase database, ManageTenant manageTenant) {
		super(type, database, manageTenant);
	}
	
	public void run () throws Exception {	
		printLine("Started-up tenant unloading tool");	
		printLine("Type set to: " + this.type);
		
		this.printLine("Setting up connection");
		this.conn = this._setupConnection();
		this.manageTenant.setConnection(this.conn);
		this.printLine("Connection setup");
		
		this._setOptions();		
		
		this._checkIfDeployed();
		
		if (this.cliArgs.hasOption("start") == false) {
			if (!this.confirmBoolean("Are you sure you want to undeploy tenant #" + this.tenantId + "? (y/n)")) {
				System.exit(ExitCodes.NO_START);
			}
		}
		
		this.printLine("Starting undeployment of tenant #" + this.tenantId);
		
		// loop through each table and delete data for tenant
		for (String tableName : this.tables.keySet()) {		
			this._deleteOldData(tableName);
		}				

		this.printLine("Finished undeployment");		
		
		this.conn.close();
		this.printLine("Stopping");
	}
		
	protected void _checkIfDeployed () throws SQLException, IOException {
		if (this.isTenantDeployed() == false) {
			this.printLine("Tenant #" + this.tenantId + " is NOT deployed!");
			System.exit(ExitCodes.NOT_DEPLOYED);
		}	
	}	

}
