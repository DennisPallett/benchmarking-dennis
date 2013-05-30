package topicus.benchmarking;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import topicus.databases.AbstractDatabase;
import topicus.databases.MonetdbDatabase;
import topicus.databases.MonetdbDatabase.MonetdbInstance;

public class BenchmarksMonetdbScript extends BenchmarksScript {
	protected MonetdbDatabase database;
	protected ArrayList<MonetdbInstance> instanceList;
	
	protected Connection conn;
	
	public BenchmarksMonetdbScript(String type, MonetdbDatabase database) {
		super(type, database);
		
		this.database = database;	
	}
	
	public ArrayList<MonetdbInstance> getInstanceList() {
		return this.instanceList;
	}
	
	public Connection getMasterConnection () {
		return this.conn;
	}
	
	protected void _setupUsers () throws Exception {
		super._setupUsers();
		
		// close connection to master (no longer needed!)
		printLine("Closing connection to master...");
		conn.close();		
		printLine("Done");
	}
	
	protected void _getDatabaseInfo () throws SQLException {				
		// setup connection
		conn = this.database.setupConnection("node1");
		
		this.tenantCount = this.database.getTenantCount(conn);
		
		this.printLine("Retrieving node count");
		this.nodeCount = this.database.getNodeCount(conn);
		this.printLine("Found " + this.nodeCount + " nodes for database");
		
		printLine("Retrieving list of instances");
		this.instanceList = database.getInstanceList(conn);
		printLine("Found " + instanceList.size() + " instances");
	}

}
