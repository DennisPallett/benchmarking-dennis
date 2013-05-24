package topicus.databases;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import topicus.benchmarking.AbstractBenchmarkRunner;
import topicus.benchmarking.BenchmarksScript;
import topicus.benchmarking.JdbcBenchmarkRunner;
import topicus.benchmarking.AbstractBenchmarkRunner.PrepareException;
import topicus.loadtenant.LoadTenantScript;

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
		return new BenchmarkRunner();
	}
			
	public int getNodeCount(Connection conn) throws SQLException {	
		Statement q = conn.createStatement();
		
		ResultSet results = q.executeQuery("SELECT COUNT(DISTINCT(hostname)) AS node_count FROM gp_segment_configuration");
		
		results.next();
		
		int nodeCount = Integer.parseInt(results.getString("node_count"));
		return nodeCount;
	}
	
	public void prepareLoadTenant (LoadTenantScript script) throws SQLException {
		script.printLine("Creating tenant partitions...");
		
		int tenantId = script.getTenantId();
		Connection conn = script.getConnection();
		Statement q = conn.createStatement();
		for(int year=2008; year < 2028; year++) {
			String partitionName = "tenant_" + tenantId + "_year_" + year;
			String value = tenantId + "" + year;
					
			script.printLine("Removing old partition for " + year + " (if exists)");						
			q.execute("ALTER TABLE fact_exploitatie DROP PARTITION IF EXISTS " + partitionName);
			
			script.printLine("Creating partition for " + year);
			q.execute("ALTER TABLE fact_exploitatie ADD PARTITION " + partitionName + " VALUES ('" + value + "') WITH (appendonly=true, orientation=column)");			
		}
		
		script.printLine("All partitions created!");
	}
	
	public int[] deployData(Connection conn, String fileName, String tableName)
			throws SQLException {
		Statement q = conn.createStatement();
			
		long start = System.currentTimeMillis();
		q.executeUpdate("COPY " + tableName + " FROM '" + fileName + "' " +
				"DELIMITER '#' NULL 'NULL'");
		int runTime = (int) (System.currentTimeMillis() - start);
		
		// determine number of rows inserted
		// unfortunately this is hard-coded
		// because Greenplum does not return row count 
		// Fortunately we know the row count for each table
		// and a COPY either inserts ALL rows or completely fails
		HashMap<String, Integer> rowCount = new HashMap<String, Integer>();
		rowCount.put("month_names",  13);
		rowCount.put("dim_tijdtabel", 36525);
		rowCount.put("dim_administratie", 28);
		rowCount.put("dim_grootboek", 22384);
		rowCount.put("dim_kostenplaats", 22735);
		rowCount.put("organisatie", 988);
		rowCount.put("closure_organisatie", 3760);
		rowCount.put("fact_exploitatie", 14961724);
		
		
		if (rowCount.containsKey(tableName) == false) {
			throw new SQLException("Unknown table `" + tableName + "`");
		}
		
		int rows = rowCount.get(tableName);
		
		return new int[]{runTime, (int)rows};
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
		//props.setProperty("prepareThreshold", "0");
		//props.setProperty("protocolVersion",  "2");
		
		if (this.user.length() > 0) {
			props.setProperty("user", this.user);
			props.setProperty("password", this.password);
		}
					
		// setup connection
		Connection conn = DriverManager.getConnection(url, props);
		
		return conn;
	}
	
	public class BenchmarkRunner extends JdbcBenchmarkRunner {
		protected  void setupConnections () throws SQLException {
			this.owner.printLine("Setting up connections for user #" + this.userId);
			
			// setup NR_OF_QUERIES connections (1 for each query)
			for(int i=0; i < NR_OF_QUERIES; i++) {
				// always node1 
				int node = 1;
				
				this.owner.printLine("Setting up connection #" + (i+1) + " to node" + node + " for user #" + this.userId);
				
				this.conns[i] = database.setupConnection("node" + node);
							
				this.owner.printLine("Connection #" + (i+1) + " setup for user #" + this.userId);
			}
		}
		
	}
	
}
