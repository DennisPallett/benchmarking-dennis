package topicus.databases;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class VerticaDatabase extends AbstractDatabase {
	
	public VerticaDatabase () {
		super();
		
		this.name = "exploitatie";
		this.user = "dbadmin";
		this.password = "test";
		this.port = 5433;		
	}
	
	public String getJdbcDriverName() {
		return "com.vertica.jdbc.Driver";
	}

	public String getJdbcUrl() {
		return "jdbc:vertica://";
	}

	public int getNodeCount(Connection conn) throws SQLException {	
		Statement stmt = conn.createStatement();
		ResultSet result = null;
		
		result = stmt.executeQuery("SELECT COUNT(*) AS node_count FROM nodes WHERE node_state = 'UP';");
		
		// retrieve node count
		result.next();
		int nodeCount = result.getInt("node_count");
		
		// close result
		result.close();
		stmt.close();
		
		return nodeCount;
	}

}
