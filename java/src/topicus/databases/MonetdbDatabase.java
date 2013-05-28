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

public class MonetdbDatabase extends AbstractDatabase {
	
	
	public MonetdbDatabase () {
		super();
		
		this.name = "master";
		this.user = "monetdb";
		this.password = "monetdb";
		this.port = 54321;	
		this.host = "node1";
	}
	
	public String getJdbcDriverName() {
		return "nl.cwi.monetdb.jdbc.MonetDriver";
	}

	public String getJdbcUrl() {
		return "jdbc:monetdb://";
	}
	
	public AbstractBenchmarkRunner createBenchmarkRunner () {
		return new BenchmarkRunner();
	}
			
	public int getNodeCount(Connection conn) throws SQLException {	
		Statement q = conn.createStatement();
		
		ResultSet results = q.executeQuery("SELECT COUNT(DISTINCT(instance_host)) AS node_count FROM instance");
		
		results.next();
		
		int nodeCount = Integer.parseInt(results.getString("node_count"));
		return nodeCount;
	}
	
	public ArrayList<MonetdbInstance> getInstanceList (Connection conn) {
		ArrayList<MonetdbInstance> list = new ArrayList<MonetdbInstance>();
		
		Statement q;
		try {
			q = conn.createStatement();
			ResultSet results = q.executeQuery("SELECT * FROM instance ORDER BY instance_id ASC");
						
			int id = 1;
			while(results.next()) {
				MonetdbInstance instance = new MonetdbInstance();
				instance.setId(id);
				instance.setPort(results.getInt("instance_port"));
				instance.setHost(results.getString("instance_host"));
				instance.setName("exploitatie");
				
				list.add(instance);
				id++;
			}			
		} catch (SQLException e) {
			e.printStackTrace();			
		}		
		
		return list;
	}
	
	public void prepareLoadTenant (LoadTenantScript script) throws SQLException {
		script.printLine("Creating tenant partitions...");
		
		Connection conn = script.getConnection();	
		Statement q = conn.createStatement();
		int tenantId = script.getTenantId();
		
		String partitionName = "tenant_" + tenantId;
		String value = String.valueOf(tenantId);
		
		script.printLine("Removing old partition for dim_grootboek");
		q.execute("ALTER TABLE dim_grootboek DROP PARTITION IF EXISTS " + partitionName);
		
		script.printLine("Creating partition for dim_grootboek");
		q.execute("ALTER TABLE dim_grootboek ADD PARTITION " + partitionName + " VALUES ('" + value + "')");
		
		script.printLine("Creating fact partitions");
		for(int year=2008; year < 2028; year++) {
			partitionName = "tenant_" + tenantId + "_year_" + year;
			value = tenantId + "" + year;
					
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
		int rows = q.executeUpdate("COPY INTO \"" + tableName + "\" FROM '" + fileName + "' " +
				"USING DELIMITERS '#','\n' NULL AS 'NULL';");
		int runTime = (int) (System.currentTimeMillis() - start);
				
		return new int[]{runTime, (int)rows};
	}
	
	public void createTable(Connection conn, DbTable table) throws SQLException {
		String tableName = table.getName();
		ArrayList<DbColumn> columns = table.getColumns();
		String partitionBy = table.partitionBy();
		String orderBy = table.orderBy();
		
		// construct query
		String query = "";
		
		query += "CREATE TABLE \"" + tableName + "\" (";
		
		ArrayList<String> colSql = new ArrayList<String>();

		DbColumn tenantCol = null;
		for(DbColumn col : columns) {
			String colQuery = "";
			
			colQuery += "\"" + col.getName() + "\"" + " ";
			
			switch(col.getType()) {
				case INTEGER:
					colQuery += "INT";
					break;
				case SMALL_INTEGER:
					colQuery += "SMALLINT";
					break;
				case VARCHAR:
					colQuery += "character";
					colQuery += " varying(" + col.getLength() + ")";
					break;
				case TIMESTAMP_WITHOUT_TIMEZONE:
					colQuery += "timestamp";
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
				colQuery += " references \"" + col.getForeignTable() + "\" (\"" + col.getForeignColumn() + "\")";
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

		query += ";";
		
		Statement q = conn.createStatement();
		q.execute(query);
		
		q.close();
	}
	
	public void dropTable(Connection conn, String tableName) throws SQLException {
		Statement q = conn.createStatement();
		
		try {
			q.execute("DROP TABLE " + tableName + " CASCADE;");
		} catch (SQLException e) {
			// ignore errors that table does not exist
			if (e.getMessage().indexOf("no such table") == -1) {
				throw e;
			}
		}
	}
	
	public Connection setupConnection (String host) throws SQLException {
		// setup connection strings
		String url = this.getJdbcUrl() + host + ":" + port;
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
	
	public Connection setupConnection () throws SQLException {
		// setup connection strings
		String url = this.getJdbcUrl() + this.host + ":" + port;
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
	
	public class MonetdbInstance extends MonetdbDatabase {
		protected int id = -1;
		
		public void setId(int id) {
			this.id = id;
		}
		
		public int getId() {
			return this.id;
		}
		
	}
	
}
