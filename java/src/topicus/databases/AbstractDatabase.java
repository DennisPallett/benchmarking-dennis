package topicus.databases;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public abstract class AbstractDatabase {
	protected String name;
	protected String user;
	protected String password;
	protected int port;
	
	public abstract String getJdbcDriverName ();
	public abstract String getJdbcUrl ();
	
	public abstract int getNodeCount(Connection conn) throws SQLException; 
	
	public abstract void dropTable (Connection conn, String tableName) throws SQLException;
	
	public abstract void createTable(Connection conn, String tableName, ArrayList<DbColumn> columns, String partitionBy) throws SQLException;
	
	public abstract int deployData(Connection conn, String fileName, String tableName) throws SQLException;
	
	public void createTable(Connection conn, String tableName, ArrayList<DbColumn> columns) throws SQLException {
		this.createTable(conn, tableName, columns, null);
	}
	
	
	public String getName () {
		return this.name;
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

}
