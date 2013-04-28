package topicus.databases;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;

import topicus.benchmarking.BenchmarksScript;

public class VoltdbDatabase extends AbstractDatabase {
	
	
	public VoltdbDatabase () {
		super();
		
		this.name = "";
		this.user = "";
		this.password = "";
		this.port = 21212;	
	}
	
	public String getJdbcDriverName() {
		return "org.voltdb.jdbc.Driver";
	}

	public String getJdbcUrl() {
		return "jdbc:voltdb://";
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
	
	public int deployData(Connection conn, String fileName, String tableName)
			throws SQLException {
		Statement q = conn.createStatement();
		int rows = q.executeUpdate("COPY " + tableName + " from '" + fileName + "' delimiter '#'" +
				" null as 'NULL' abort on error DIRECT;");
		return rows;
	}
	
	public void createTable(Connection conn, DbTable table) throws SQLException {
		throw new SQLException("Not supported by VoltDB");
	}
	
	public void dropTable(Connection conn, String tableName) throws SQLException {
		throw new SQLException("Not supported by VoltDB");
	}

}
