package topicus.databases;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import topicus.benchmarking.AbstractBenchmarkRunner;
import topicus.benchmarking.BenchmarksScript;
import topicus.benchmarking.JdbcBenchmarkRunner;
import topicus.benchmarking.AbstractBenchmarkRunner.PrepareException;

public class GreenplumDatabase extends AbstractDatabase {
	
	
	public GreenplumDatabase () {
		super();
		
		this.name = "exploitatie";
		this.user = "gpadmin";
		this.password = "";
		this.port = 5432;	
	}
	
	public String getJdbcDriverName() {
		return "org.postgresql.Driver";
	}

	public String getJdbcUrl() {
		return "jdbc:postgresql://";
	}
	
	public AbstractBenchmarkRunner createBenchmarkRunner () {
		return new JdbcBenchmarkRunner();
	}
			
	public int getNodeCount(Connection conn) throws SQLException {	
		return 1;
	}
	
	public int[] deployData(Connection conn, String fileName, String tableName)
			throws SQLException {
		Statement q = conn.createStatement();
		
		long start = System.currentTimeMillis();
		int rows = q.executeUpdate("COPY " + tableName + " FROM'" + fileName + "' " +
				"DELIMITER '#' NULL 'NULL'");
		int runTime = (int) (System.currentTimeMillis() - start);
		
		return new int[]{runTime, rows};
	}
	
	public void createTable(Connection conn, DbTable table) throws SQLException {
		String tableName = table.getName();
		ArrayList<DbColumn> columns = table.getColumns();
		String partitionBy = table.partitionBy();
		String orderBy = table.orderBy();
		
		// construct query
		String query = "";
		
		query += "CREATE TABLE " + tableName + " (";
		
		ArrayList<String> colSql = new ArrayList<String>();

		DbColumn tenantCol = null;
		for(DbColumn col : columns) {
			String colQuery = "";
			
			colQuery += col.getName() + " ";
			
			switch(col.getType()) {
				case INTEGER:
					colQuery += "integer";
					break;
				case SMALL_INTEGER:
					colQuery += "smallint";
					break;
				case VARCHAR:
					colQuery += "character";
					colQuery += " varying(" + col.getLength() + ")";
					break;
				case TIMESTAMP_WITHOUT_TIMEZONE:
					colQuery += "timestamp without time zone";
					break;
				case DOUBLE:
					colQuery += "double precision";
					break;
				default:
					throw new SQLException("Unknown column type: " + col.getType());
			}
						
			if (col.isPrimaryKey()) {
				colQuery += " primary key";
			} else if (col.isUnique()) {
				// plum does not support unique
			} else if (col.isForeignKey()) {
				colQuery += " references " + col.getForeignTable() + "(" + col.getForeignColumn() + ")";
			}
			
			if (col.allowNull() == false) {
				colQuery += " not null";
			}
			
			if (col.isTenantKey()) {
				tenantCol = col;
			}
			
			colSql.add(colQuery);
		}
		
		query += StringUtils.join(colSql, ", ");
		
		query += ")";
		
		/*if (orderBy != null) {
			query += " ORDER BY " + orderBy;
		}*/
		
		/*if (table.getName().equals("fact_exploitatie")) {
			query += " DISTRIBUTE BY APPEND (tenant_year_key)";
		} else if (partitionBy != null) {
			query += " DISTRIBUTE BY APPEND (" + partitionBy + ")";
		} else if (tenantCol != null) {
			query += " DISTRIBUTE BY APPEND (" + tenantCol.getName() + ")";
		} else if (table.getName().equals("month_names")) {
			query += " DISTRIBUTE BY APPEND (month_name)";
		} else if (table.getName().equals("dim_tijdtabel")) {
			query += " DISTRIBUTE BY APPEND (pk_date)";
		} else if (table.getName().equals("closure_organisatie")) {
			query += " DISTRIBUTE BY APPEND (organisatie_key)";
		}*/
		
		query += ";";
		
		Statement q = conn.createStatement();
		q.execute(query);
		
		q.close();
	}
	
	public void dropTable(Connection conn, String tableName) throws SQLException {
		Statement q = conn.createStatement();
		q.execute("DROP TABLE IF EXISTS " + tableName + " CASCADE;");
	}

	public Connection setupConnection (String host) throws SQLException {
		// setup connection strings
		String url = this.getJdbcUrl() + host + ":" + this.port;
		if (this.name.length() > 0) {
			url += "/" + this.name;
		}
		
		Properties props = new Properties();
		props.setProperty("prepareThreshold", "0");
		props.setProperty("protocolVersion",  "2");
		
		if (this.user.length() > 0) {
			props.setProperty("user", this.user);
			props.setProperty("password", this.password);
		}
					
		// setup connection
		Connection conn = DriverManager.getConnection(url, props);
		
		return conn;
	}
	
}
