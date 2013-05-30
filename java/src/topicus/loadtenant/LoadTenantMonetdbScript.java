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
import topicus.databases.DbTable;
import topicus.databases.MonetdbDatabase;
import topicus.databases.MonetdbDatabase.MonetdbInstance;
import topicus.loadschema.DbSchema;

public class LoadTenantMonetdbScript extends LoadTenantScript {
	protected MonetdbDatabase database;

	public LoadTenantMonetdbScript(String type, MonetdbDatabase database, ManageTenant manageTenant) {
		super(type, database, manageTenant);
		
		this.database = database;
	}
	
	public void run () throws Exception {	
		printLine("Started-up tenant data loading tool for MonetDB");	
		
		this.setupLogging(cliArgs.getOptionValue("log-file"));
		
		printLine("Type set to: " + this.type);
		
		this.printLine("Setting up connection to master");
		this.conn = this._setupConnection();
		this.manageTenant.setConnection(this.conn);
		this.printLine("Connection setup");
		
		this._setOptions();		
			
		this._checkIfDeployed();
		
		// get instance for tenant
		MonetdbInstance instance = database.getInstanceForTenant(conn, this.tenantId);
		
		// get connection for instance
		printLine("Setting up connection to instance " + instance.getDisplayName());
		Connection instanceConn = instance.setupConnection();
		printLine("Connection setup");
		
		this.printLine("Starting deployment of tenant data for tenant #" + this.tenantId);
			
		// loop through tables and deploy
		for(Map.Entry<String, String> entry : this.tables.entrySet()) {
			String tableName = entry.getKey();
			String fileName = entry.getValue();
			
			if (tableName.equals("fact_exploitatie")) {
				this._deployFactData(instance, instanceConn, fileName);
			} else {
				this._deployData(instance, instanceConn, fileName, tableName);
			}
		}		
		
		// notify master about new tenant
		printLine("Adding tenant to master...");
		PreparedStatement q = conn.prepareStatement("INSERT INTO tenant (tenant_id, tenant_host, tenant_port) VALUES (?, ?, ?)");
		q.setInt(1,  this.tenantId);		
		q.setString(2, instance.getHost());
		q.setInt(3, instance.getPort());
		q.executeUpdate();
		printLine("Done!");
		
		this.printLine("Finished deployment of tenant data");		
		
		instanceConn.close();
		this.conn.close();
		this.database.close();
		this.printLine("Stopping");
	}
	
	protected void _deployFactData (MonetdbInstance instance, Connection conn, String fileName) throws SQLException {
		long start = System.currentTimeMillis();
		
		DbTable factTable = DbSchema.FactTable();
		
		// delete parent table (if already exists)
		database.dropTable(conn, factTable);
		
		// create parent table
		database.createTable(conn,  factTable);
		
		// insert all data into parent table
		printLine("Deploying " + fileName + " into parent fact table...");
		int[] ret = instance.deployData(conn, this.tenantDirectory + fileName, factTable.getName());
		printLine("Done, deployed " + ret[1] + " rows into parent fact table");
			
		// create partition for each year and load data from parent table into partitions
		int totalRowCount = 0;
		for(int year=2008; year <= 2027; year++) {
			String newName = "fact_exploitatie_tenant_" + this.tenantId + "_year_" + year;
			factTable.setName(newName);
			
			// create partition
			printLine("Creating partition '" + newName + "'...");
			instance.dropTable(conn, factTable);
			instance.createTable(conn, factTable);
			printLine("Partition created");
			
			// load data into partition from parent table
			printLine("Loading data from parent table into partition...");
			PreparedStatement q = conn.prepareStatement(
				"INSERT INTO \"" + newName + "\" SELECT * FROM fact_exploitatie " +
				"WHERE tenant_key = ? AND year_key = ?"
			);
			q.setInt(1,  this.tenantId);
			q.setInt(2, year);
			int rowCount = q.executeUpdate();
			printLine("Loaded " + rowCount + " rows into partition!");
			
			totalRowCount += rowCount;
		}
		
		// delete parent table again (no longer needed)
		database.dropTable(conn, "fact_exploitatie");
		
		int runTime = (int) (System.currentTimeMillis() - start);
		
		this.printLine("Inserted " + totalRowCount + " rows");		
		this.addResult("fact_exploitatie", totalRowCount, runTime);	
	}
	
	protected boolean isTenantDeployed () throws SQLException {
		PreparedStatement q = this.conn.prepareStatement("SELECT * FROM tenant WHERE tenant_id = ?");
		q.setInt(1, this.tenantId);	
		q.execute();
		
		ResultSet results = q.getResultSet();
		
		boolean ret = (results.next() && results.getInt("tenant_id") == tenantId);
		
		return ret;	
	}
	
	
}
