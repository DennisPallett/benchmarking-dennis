package topicus.databases;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractDatabase {
	protected String name;
	protected String user;
	protected String password;
	protected int port;
	
	public abstract String getJdbcDriverName ();
	public abstract String getJdbcUrl ();
	
	public abstract int getNodeCount(Connection conn) throws SQLException; 
	
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
