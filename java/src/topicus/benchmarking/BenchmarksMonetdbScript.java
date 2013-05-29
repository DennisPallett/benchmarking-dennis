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
	
	public BenchmarksMonetdbScript(String type, MonetdbDatabase database) {
		super(type, database);
		
		this.database = database;	
	}
	
	public ArrayList<MonetdbInstance> getInstanceList() {
		return this.instanceList;
	}
	
	protected void _getDatabaseInfo () throws SQLException {				
		// setup connection
		Connection conn = this.database.setupConnection("node1");
		
		this.tenantCount = this.database.getTenantCount(conn);
		
		this.printLine("Retrieving node count");
		this.nodeCount = this.database.getNodeCount(conn);
		this.printLine("Found " + this.nodeCount + " nodes for database");
		
		printLine("Retrieving list of instances");
		this.instanceList = database.getInstanceList(conn);
		printLine("Found " + instanceList.size() + " instances");
		
		conn.close();
	}

}
