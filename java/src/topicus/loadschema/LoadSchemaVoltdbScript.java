package topicus.loadschema;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.benchmarking.BenchmarksScript.MissingResultsFileException;
import topicus.databases.AbstractDatabase;
import topicus.databases.DbColumn;
import topicus.databases.DbTable;
import topicus.loadtenant.LoadTenantScript.AlreadyDeployedException;

public class LoadSchemaVoltdbScript extends LoadSchemaScript {
	protected String baseDirectory = "";	
	protected String outputDirectory = "";
	protected String queriesFile = "";
	
	protected Connection conn;

	public LoadSchemaVoltdbScript(String type, AbstractDatabase database) {
		super(type, database);
	}
	
	public void run () throws Exception {	
		printLine("Started-up schema loading tool for VoltDB");	
		printLine("This script will create the schema file for VoltDB");
		printLine("Which can be compiled into a catalog file");
		
		this.setupLogging(cliArgs.getOptionValue("log-file"));
			
		this._setOptions();
		
		printLine("Ready to create schema");
		
		if (!cliArgs.hasOption("start")) {
			if (!confirmBoolean("Start with deployment of schema and base data? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		//this._checkIfDeployed();		
			
		this.printLine("Starting deployment of schema");
		
		StringBuilder output = new StringBuilder();
		
		output.append(_createSchemaFile());
		//output.append(_createQueries());
		
		File file = new File(this.outputDirectory + "schema.sql");
		FileUtils.writeStringToFile(file,  output.toString());

		this.printLine("Finished deployment");			
		this.printLine("Stopping");
	}
	
	protected StringBuilder _createSchemaFile () throws SQLException {
		ArrayList<DbTable> tables = DbSchema.AllTables();
		
		StringBuilder schema = new StringBuilder();
		for(DbTable table : tables) {
			schema.append(_createTable(table));
			schema.append("\n\n");
		}
		
		return schema;
	}
	
	protected String _createQueries() throws IOException {
		ArrayList<String> procList = new ArrayList<String>();
		
		CSVReader reader = new CSVReader(new FileReader(queriesFile), ',', '"');
		List<String[]> queryList = reader.readAll();
		
		for(int setId=1; setId <= queryList.size(); setId++) {
			String[] set = queryList.get(setId-1);
			
			for(int queryId=1; queryId <= set.length; queryId++) {
				String query = set[queryId-1];
				String procName = "set_" + setId + "_query_" + queryId;
				
				String proc = "CREATE PROCEDURE " + procName + " AS " + query;
				procList.add(proc);				
			}
			
		}

		reader.close();
		
		return StringUtils.join(procList.iterator(), "\n");
	}
	
	protected StringBuilder _createTable(DbTable table) throws SQLException {
		StringBuilder ret = new StringBuilder();
		
		ret.append("CREATE TABLE " + table.getName() + " (\n");
		
		ArrayList<DbColumn> columns = table.getColumns();
		String[] colDefs = new String[columns.size()];
		for(int i=0; i < columns.size(); i++) {
			DbColumn col = columns.get(i);
			StringBuilder colDef = new StringBuilder();
			
			colDef.append(col.getName() + " ");
			
			switch(col.getType()) {
				case DOUBLE:
					colDef.append("FLOAT");
					break;
				case INTEGER:
					colDef.append("INTEGER");
					break;
				case SMALL_INTEGER:
					colDef.append("SMALLINT");
					break;
				case TIMESTAMP:
				case TIMESTAMP_WITHOUT_TIMEZONE:
					colDef.append("VARCHAR(20)");
					break;
				case VARCHAR:
					colDef.append("VARCHAR(" + col.getLength() + ")");
					break;
				default:
					throw new SQLException("Unknown column type `" + col.getType() + "`");
			}
			
			if (col.isUnique()) {
				colDef.append(" UNIQUE");
			}
			
			if (col.allowNull() == false) {
				colDef.append(" NOT NULL");
			}		
			
			colDefs[i] = colDef.toString();
		}		
		
		ret.append(StringUtils.join(colDefs, ",\n"));
		
		// add primary key
		for(DbColumn col : columns)  {
			if (col.isPrimaryKey()) {
				ret.append(",\n");
				ret.append("PRIMARY KEY (" + col.getName() + ")");
			}
		}
				
		ret.append("\n);");
		
		if (table.partitionBy() != null) {
			ret.append("\n");
			ret.append("PARTITION TABLE " + table.getName() + " ON COLUMN " + table.partitionBy() + ";");
		}
		
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
		
		this.outputDirectory = cliArgs.getOptionValue("output-directory", "");
		if (this.outputDirectory.length() == 0) {
			throw new InvalidOutputDirectoryException("Missing output directory!");
		}
		File outputDir = new File(this.outputDirectory);
		if (outputDir.exists() == false) {
			throw new InvalidOutputDirectoryException("Output directory `" + this.outputDirectory + "` does not exist!");
		}
		this.outputDirectory = outputDir.getAbsolutePath() + "/";
		printLine("Output directory set to: " + this.outputDirectory);
		
		this.queriesFile = cliArgs.getOptionValue("queries", "");
		if (this.queriesFile.length() == 0) {
			throw new InvalidQueriesException("Missing queries file!");
		}
		File qFile = new File(queriesFile);
		if (qFile.exists() == false) {
			throw new InvalidQueriesException("Queries file `" + queriesFile + "` does not exist!");
		}
		printLine("Queries file set to: " + queriesFile);
	}
	
	public class InvalidQueriesException extends Exception {
		public InvalidQueriesException(String string) {
			super(string);
		}
	}
	
	public class InvalidOutputDirectoryException extends Exception {
		public InvalidOutputDirectoryException(String string) {
			super(string);
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
	
	/*
	 * printLine("Setting up SSH connection with node1");
		
		JSch jsch = new JSch();
		jsch.addIdentity(System.getProperty("user.home") + "/ssh_host_rsa_key");
		Session session = jsch.getSession("root", "node1");
		session.setConfig("StrictHostKeyChecking", "no");
		
		session.connect();
		
		printLine("yes");
		
		Channel channel = session.openChannel("exec");
			
		((ChannelExec)channel).setCommand("echo \"Hi!\"; echo \"I am $MY_NAME\"");
		
		channel.setInputStream(null);
		((ChannelExec)channel).setErrStream(System.err);
		
		InputStream in=channel.getInputStream();
		
		channel.connect();
		
		byte[] tmp=new byte[1024];
	      while(true){
	        while(in.available()>0){
	          int i=in.read(tmp, 0, 1024);
	          if(i<0)break;
	          System.out.print(new String(tmp, 0, i));
	        }
	        if(channel.isClosed()){
	          System.out.println("exit-status: "+channel.getExitStatus());
	          break;
	        }
	        try{Thread.sleep(1000);}catch(Exception ee){}
	      }
	      channel.disconnect();
	      session.disconnect();printLine("Setting up SSH connection with node1");
		
		JSch jsch = new JSch();
		jsch.addIdentity(System.getProperty("user.home") + "/ssh_host_rsa_key");
		Session session = jsch.getSession("root", "node1");
		session.setConfig("StrictHostKeyChecking", "no");
		
		session.connect();
		
		printLine("yes");
		
		Channel channel = session.openChannel("exec");
			
		((ChannelExec)channel).setCommand("echo \"Hi!\"; echo \"I am $MY_NAME\"");
		
		channel.setInputStream(null);
		((ChannelExec)channel).setErrStream(System.err);
		
		InputStream in=channel.getInputStream();
		
		channel.connect();
		
		byte[] tmp=new byte[1024];
	      while(true){
	        while(in.available()>0){
	          int i=in.read(tmp, 0, 1024);
	          if(i<0)break;
	          System.out.print(new String(tmp, 0, i));
	        }
	        if(channel.isClosed()){
	          System.out.println("exit-status: "+channel.getExitStatus());
	          break;
	        }
	        try{Thread.sleep(1000);}catch(Exception ee){}
	      }
	      channel.disconnect();
	      session.disconnect();
	      */
	 

	
}
