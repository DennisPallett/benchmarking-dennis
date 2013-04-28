package topicus.loadschema;

import java.io.File;
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
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.HostKeyRepository;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

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
	
	protected Connection conn;

	public LoadSchemaVoltdbScript(String type, AbstractDatabase database) {
		super(type, database);
	}
	
	public void run () throws Exception {	
		printLine("Started-up schema loading tool for VoltDB");	
		
		this.setupLogging(cliArgs.getOptionValue("log-file"));
		
		printLine("Type set to: " + this.type);
			
		this._setOptions();
		
		printLine("Ready to deploy schema & base data");
		
		if (!cliArgs.hasOption("start")) {
			if (!confirmBoolean("Start with deployment of schema and base data? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		//this._checkIfDeployed();
		
		printLine("Setting up SSH connection with node1");
		
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
		
		
			
		this.printLine("Starting deployment of schema");
		

		this.printLine("Finished deployment");			
		this.printLine("Stopping");
	}
	
	protected void _deploySchema() throws SQLException {
		ArrayList<DbTable> tables = DbSchema.AllTables();
		
		for(DbTable table : tables) {
			this._deployTable(table);
		}
	}
	
	protected void _deployTable(DbTable table) throws SQLException {
		this.printLine("Deploying `" + table.getName() + "`");
		
		database.dropTable(conn, table);
		
		this.database.createTable(this.conn, table);		
		this.printLine("Table deployed");	
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
