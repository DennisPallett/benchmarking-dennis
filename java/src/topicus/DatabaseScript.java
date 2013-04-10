package topicus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;

import topicus.databases.AbstractDatabase;

public abstract class DatabaseScript extends ConsoleScript {
	protected static final int EXIT_INVALID_JDBC_DRIVER = 9;
	
	public boolean dbConfigLoaded = false;
	public String dbUser = null;
	public String dbName = null;
	public String dbPassword = null;
	public int dbPort = 0;
		
	protected AbstractDatabase database;
	protected String type;
	
	public abstract void run() throws Exception;
	
	public DatabaseScript (String type, AbstractDatabase database) {
		this.type = type;
		this.database = database;
	}
	
	public String getJdbcUrl () {
		return this.database.getJdbcUrl();
	}
	
	protected Connection _setupConnection () throws Exception {	
		if (!this.dbConfigLoaded) {
			this._loadDbConfig();
		}
		
		// setup connection strings
		String url = this.database.getJdbcUrl() + "node1:" + this.dbPort + "/" + this.dbName;
		this.printLine("Connection string: " + url);
					
		// setup connection
		return DriverManager.getConnection(url, this.dbUser, this.dbPassword);
	}

	protected void _loadDbConfig () throws Exception {
		// load JDBC driver
		String driver = this.database.getJdbcDriverName();
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			this.printError("Unable to load JDBC driver for " + this.type);
			System.exit(ExitCodes.INVALID_JDBC_DRIVER);
		}
		
		this.dbName = cliArgs.getOptionValue("database", this.database.getName());
		if (this.dbName == null || this.dbName.length() == 0) {
			this.printError("No database specified");
			System.exit(-1);
		}
		
		this.dbUser = cliArgs.getOptionValue("database-user", this.database.getUser());
		if (this.dbUser == null || this.dbUser.length() == 0) {
			this.printError("No database user specified");
			System.exit(-1);
		}
		
		this.dbPassword = cliArgs.getOptionValue("database", this.database.getPassword());
		if (this.dbPassword == null || this.dbName.length() == 0) {
			this.dbPassword = "";
		}
		
		this.dbPort = Integer.parseInt(cliArgs.getOptionValue("port", String.valueOf(this.database.getPort())));
		if (this.dbPort == 0) {
			this.printError("No database port specified");
			System.exit(-1);
		}
		
		this.dbConfigLoaded = true;		
	}
	
}
