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
import java.util.Map;

import org.apache.commons.cli.CommandLine;

import au.com.bytecode.opencsv.CSVWriter;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.benchmarking.BenchmarksScript.MissingResultsFileException;
import topicus.databases.AbstractDatabase;
import topicus.databases.DbColumn;
import topicus.databases.DbTable;
import topicus.loadtenant.LoadTenantScript.AlreadyDeployedException;

public class LoadBaseDataScript extends DatabaseScript {
	protected String baseDirectory = "";	
	
	protected Connection conn;

	public LoadBaseDataScript(String type, AbstractDatabase database) {
		super(type, database);
	}
	
	public void run () throws Exception {	
		printLine("Started-up base-data loading tool");	
		
		this.setupLogging(cliArgs.getOptionValue("log-file"));
		
		printLine("Type set to: " + this.type);
		
		this.printLine("Setting up connection");
		this.conn = this._setupConnection();
		this.printLine("Connection setup");
		
		this._setOptions();
		
		if (cliArgs.hasOption("start") == false) {
			if (confirmBoolean("Ready to load base-data. Continue? (y/n)") == false) {
				throw new CancelledException("Cancelled by user");
			}
		}
					
		this.printLine("Starting loading of base-data");
		
		this._loadBaseData();

		this.printLine("Finished loading!");		
		
		this.conn.close();
		this.printLine("Stopping");
	}
	
	
	protected void _loadBaseData () throws SQLException {
		printLine("Loading base data");
		
		this.printLine("Loading month names...");
		database.deployData(conn, this.baseDirectory + "month_names.tbl", "month_names");
		this.printLine("Done");		
		
		this.printLine("Loading tijdtabel data...");
		database.deployData(conn, this.baseDirectory + "tijd_data.tbl", "dim_tijdtabel");
		this.printLine("Done");		
		
		printLine("Finished loading base data");
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
