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
import topicus.databases.MonetdbDatabase;
import topicus.databases.DbColumn;
import topicus.databases.DbTable;
import topicus.databases.MonetdbDatabase.MonetdbInstance;
import topicus.loadtenant.LoadTenantScript.AlreadyDeployedException;

public class LoadBaseDataMonetdbScript extends LoadBaseDataScript {
	protected MonetdbDatabase database;

	public LoadBaseDataMonetdbScript(String type, MonetdbDatabase database) {
		super(type, database);
		this.database = database;
	}	
	
	protected void _loadBaseData () throws SQLException {
		printLine("Loading base data");
		
		ArrayList<MonetdbInstance> instanceList = database.getInstanceList(conn);
		
		if (instanceList.size() == 0) {
			throw new SQLException("No instances to deploy schema to!");
		}
		
		for(MonetdbInstance instance : instanceList) {
			String hostName = instance.getHost();
			int port = instance.getPort();
			String instanceName = "#" + instance.getId() + " (" + hostName + ":" + port + ")";
			
			printLine("Setting up connection to instance " + instanceName);
			Connection instanceConn = instance.setupConnection();
			
			super._loadBaseData(instance, instanceConn);	
			
			printLine("Deployed base-data on instance " + instanceName);
			
			printLine("Closing connection");
			instanceConn.close();
		}
				
		printLine("Finished loading base data");
	}
	
}
