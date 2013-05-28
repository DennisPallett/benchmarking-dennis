package topicus.loadschema;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;

import au.com.bytecode.opencsv.CSVWriter;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.benchmarking.BenchmarksScript.MissingResultsFileException;
import topicus.databases.AbstractDatabase;
import topicus.databases.MonetdbDatabase;
import topicus.databases.DbColumn;
import topicus.databases.DbTable;
import topicus.databases.MonetdbDatabase.MonetdbInstance;
import topicus.loadtenant.LoadTenantScript.AlreadyDeployedException;

public class LoadSchemaMonetdbScript extends LoadSchemaScript {
	protected String baseDirectory = "";	
	
	protected Connection conn;

	protected MonetdbDatabase database = null;
	
	public LoadSchemaMonetdbScript(String type, MonetdbDatabase database) {
		super(type, database);
		this.database = database;
	}
	
	public void run () throws Exception {	
		printLine("Started-up schema loading tool for MonetDB");	
		
		this.setupLogging(cliArgs.getOptionValue("log-file"));
		
		printLine("Type set to: " + this.type);
		
		this.printLine("Setting up master connection");
		this.conn = this._setupConnection();
		this.printLine("Connection setup");
		
		ArrayList<MonetdbInstance> instanceList = database.getInstanceList(conn);
		
		if (instanceList.size() == 0) {
			throw new Exception("No instances to deploy schema to!");
		}
					
		this.printLine("Starting deployment of schema");
		
		for(MonetdbInstance instance : instanceList) {
			String hostName = instance.getHost();
			int port = instance.getPort();
			String instanceName = "#" + instance.getId() + " (" + hostName + ":" + port + ")";
			
			printLine("Setting up connection to instance " + instanceName);
			Connection instanceConn = instance.setupConnection();
			
			this._deploySchema(instanceConn, instance);	
			
			printLine("Deployed schema on instance " + instanceName);
			
			printLine("Closing connection");
			instanceConn.close();
		}
		
		this.printLine("Finished deployment");		
		
		this.conn.close();
		this.printLine("Stopping");
	}
	
	protected void _deploySchema(Connection conn, AbstractDatabase database) throws SQLException {
		ArrayList<DbTable> tables = DbSchema.AllTables();
		
		for(DbTable table : tables) {
			if (table.getName().equals("fact_exploitatie") == false) {
				this._deployTable(conn, database, table);
			}
		}
	}
	
	protected void _deployTable(Connection conn, AbstractDatabase database, DbTable table) throws SQLException {
		this.printLine("Deploying `" + table.getName() + "`");
		
		database.dropTable(conn, table);
		
		this.database.createTable(conn, table);		
		this.printLine("Table deployed");	
	}
	

	
	protected void _checkIfDeployed () throws Exception {
		if (this.isSchemaDeployed()) {
			this.printError("Schema is already deployed!");
			
			if (this.cliArgs.hasOption("stop-on-deployed")) {
				throw new AlreadyDeployedException("Schema is already deployed");
			} else {
				boolean doDeploy = this.confirmBoolean("Are you sure you want to re-deploy schema? This will delete any old data! (y/n)");
				if (!doDeploy) {
					printLine("Stopping");
					throw new CancelledException("Cancelled by user");
				}				
			}
		}
	}
	
	protected boolean isSchemaDeployed () throws SQLException {
		// do a simple count on month names to determine table exists
		Statement q = this.conn.createStatement();
		
		ResultSet result = null;
		try {
			result = q.executeQuery("SELECT * FROM month_names LIMIT 1");
		} catch (SQLException e) {
			// fail, likely because no table there
		}
		
		boolean ret = (result != null && result.next());
		
		if (result != null) result.close();
		
		return ret;
	}
	
	protected void _setOptions () throws Exception {	
		this.baseDirectory = this.cliArgs.getOptionValue("base-data", "/benchmarking/data/base-data/");
		if (this.baseDirectory.length() == 0) {
			throw new MissingBaseDataDirectoryException("Missing base data directory!");
		}
		
		if (this.baseDirectory.endsWith("/") == false
			&& this.baseDirectory.endsWith("\\") == false) {
				this.baseDirectory += "/";
		}
	}
	
	public class CancelledException extends Exception {
		public CancelledException(String string) {
			super(string);
		}
	}
	
	public class MissingBaseDataDirectoryException extends Exception {
		public MissingBaseDataDirectoryException(String string) {
			super(string);
		}
	}
	
	public class AlreadyDeployedException extends Exception {
		public AlreadyDeployedException(String string) {
			super(string);
		}
	}

	
}
