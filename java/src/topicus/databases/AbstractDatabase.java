package topicus.databases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import topicus.ConsoleScript;
import topicus.benchmarking.BenchmarksScript;
import topicus.benchmarking.AbstractBenchmarkRunner;
import topicus.loadtenant.LoadTenantScript;

public abstract class AbstractDatabase extends Observable {
	protected String name;
	protected String user;
	protected String password;
	protected int port;
	protected String host;
	

	public abstract String getJdbcDriverName ();
	public abstract String getJdbcUrl ();
	
	public abstract int getNodeCount(Connection conn) throws SQLException; 
	public abstract void dropTable (Connection conn, String tableName) throws SQLException;
	public abstract void createTable(Connection conn, DbTable table) throws SQLException;
	public abstract int[] deployData(Connection conn, String fileName, String tableName) throws SQLException;
	
	public abstract AbstractBenchmarkRunner createBenchmarkRunner();
		
	public Connection setupConnection (String host) throws SQLException {
		// setup connection strings
		String url = this.getJdbcUrl() + host + ":" + this.port;
		if (this.name.length() > 0) {
			url += "/" + this.name;
		}
					
		// setup connection
		Connection conn = null;
		if (this.user.length() > 0) {
			conn = DriverManager.getConnection(url, this.user, this.password);
		} else {
			conn = DriverManager.getConnection(url);
		}
		
		return conn;
	}
	
	public void prepareLoadTenant (LoadTenantScript script) throws Exception {
		// do something
	}
	
	public void finishLoadTenant (LoadTenantScript script) throws Exception {
		// do something
	}
	
	public void prepareUnloadTenant () {
		// do something
	}
	
	public void finishUnloadTenant () {
		// do something
	}
	
	public void close () {
		// do something
	}
	
	public void dropTable(Connection conn, DbTable table) throws SQLException {
		this.dropTable(conn, table.getName());
	}
		
	public String prepareQuery(String query) {
		return query;
	}
	
	public String addQueryLabel(String query, String benchmarkId, int userId, int iteration, int setId, int queryId) {
		return query;
	}
		
	public String getName () {
		return this.name;
	}
	
	public void setName (String name) {
		this.name = name;
	}
	
	public String getUser () {
		return this.user;
	}
	
	public String getPassword () {
		return this.password;
	}
	
	public int getPort () {
		return this.port;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public String getHost () {
		return this.host;
	}
	
	public void printLine (String msg) {
		this.setChanged();
		this.notifyObservers(msg);
	}
	
	public class TimeoutException extends Exception {}

}
