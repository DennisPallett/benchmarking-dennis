package topicus.databases;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import topicus.benchmarking.AbstractBenchmarkRunner;
import topicus.benchmarking.BenchmarksScript;
import topicus.benchmarking.JdbcBenchmarkRunner;

public class VoltdbDatabase extends AbstractDatabase {	
	public VoltdbDatabase () {
		super();
		
		this.name = "";
		this.user = "";
		this.password = "";
		this.port = 21212;	
	}
	
	public AbstractBenchmarkRunner createBenchmarkRunner () {
		return new BenchmarkRunner();
	}
	
	public void close () {

	}
	
	public String getJdbcDriverName() {
		return "org.voltdb.jdbc.Driver";
	}

	public String getJdbcUrl() {
		return "jdbc:voltdb://";
	}
		
	public int getNodeCount(Connection conn) throws SQLException {	
		Statement stmt = conn.createStatement();
		ResultSet results = null;
		
		CallableStatement proc = conn.prepareCall("{call @SystemInformation(?)}");
		proc.setString(1,  "DEPLOYMENT");
		results = proc.executeQuery();
		
		int nodeCount = 0;
		while(results.next()) {
			String property = results.getString(1);
			String value = results.getString(2);
			
			if (property.equals("hostcount")) {
				nodeCount = Integer.parseInt(value);
			}
		}
		
		results.close();
		stmt.close();
		
		return nodeCount;
	}
	
	public int[] deployData(Connection conn, String fileName, String tableName) throws SQLException {
		long offset = 0;
		
		int totalRowCount = 0;
		int runTime = 0;
		
		NumberFormat formatter = NumberFormat.getInstance();
		DecimalFormat df = new DecimalFormat("#.##");
		
		long startTime = System.currentTimeMillis();
		while(offset > -1) {
			CallableStatement proc = conn.prepareCall("{call BulkLoad(?, ?, ?)}");
			proc.setString(1, tableName);
			proc.setString(2, fileName);
			proc.setLong(3,  offset);
			
			ResultSet results = proc.executeQuery();
			
			results.next();
			
			offset = results.getLong("offset"); 
			int rowCount = results.getInt("rowcount");
			double execTime = (double)results.getInt("exectime")/1000;			
			
			totalRowCount = totalRowCount + rowCount;
			
			if (rowCount > 0) {
				printLine("Inserted " + formatter.format(rowCount) + " rows (total row count: " + formatter.format(totalRowCount) + ") " +
					"in approx. " + df.format(execTime) + " seconds");
			}
			
			results.close();
			proc.close();
		}	
		
		runTime = (int) ((int) System.currentTimeMillis() - startTime);
		
		return new int[]{runTime, totalRowCount};
	}
		
	public void createTable(Connection conn, DbTable table) throws SQLException {
		throw new SQLException("Not supported by VoltDB");
	}
	
	public void dropTable(Connection conn, String tableName) throws SQLException {
		throw new SQLException("Not supported by VoltDB");
	}
	
	public class BenchmarkRunner extends AbstractBenchmarkRunner {
		protected Client client;

		@Override
		public void prepareBenchmark() throws PrepareException {
			try {
				this.setupClient();
			} catch (Exception e) {
				throw new PrepareException(e.getClass().getName() + ": " + e.getMessage());
			}		
		}

		@Override
		public void runIteration(int iteration) {
			Iterator<String[]> iter = this.queryList.iterator();
			
			int setNum = 1;
			while(iter.hasNext()) {
				String[] setInfo = iter.next();
				String procName = setInfo[0];
				int yearKey = Integer.parseInt(setInfo[1]);
				String parentId = setInfo[2];
				
				parentId = this._replaceOrgId(parentId);
				
				long tenantYearKey = (this.userId * 10000) + yearKey;
				
				long startTime = System.currentTimeMillis();
				ClientResponse response = null;
				boolean timeout = false;
				boolean failed = false;
				
				try {
					response = client.callProcedure(procName,  tenantYearKey, parentId);
				} catch (ProcCallException e) {
					response = e.getClientResponse();
					if (response != null && response.getStatus() == ClientResponse.CONNECTION_TIMEOUT) {
						timeout = true;
						owner.printError("Set #" + setNum + " timed out");
					} else {
						e.printStackTrace();
						failed = true;
					}
				} catch (Exception e) {
					e.printStackTrace();
					failed = true;					
				}
				
				int setTime = (int) (System.currentTimeMillis() - startTime);
				int queryTime = setTime/4;
				
				if (timeout) {
					setTime = BenchmarksScript.QUERY_TIMEOUT;
					queryTime = BenchmarksScript.QUERY_TIMEOUT;
				}
				
				// save results
				if (failed == false) {
					this.owner.addResult(
						this.userId, 
						iteration, 
						setNum,
						queryTime,
						queryTime,
						queryTime,
						queryTime,
						setTime
					);
				}
				
				setNum++;
				
				// rate limiting
				// to avoid flooding the database
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}

		@Override
		public void finishBenchmark() {
			this.owner.printLine("User #" + this.userId + " closing client");
			
			try {
				client.close();
			} catch (InterruptedException e) {
				// don't care
			}
					
			this.owner.printLine("Client for user #" + this.userId + " closed");			
		}
		
		protected void setupClient () throws UnknownHostException, IOException {
			// determine node
			int node = (this.userId % this.nodes) + 1;
						
			this.owner.printLine("Setting up client for user #" + this.userId + " to node" + node);
			
			// setup config
			ClientConfig config = new ClientConfig(null, null);
			config.setProcedureCallTimeout(BenchmarksScript.QUERY_TIMEOUT);
			config.setConnectionResponseTimeout(BenchmarksScript.QUERY_TIMEOUT);
			
			// create Client object
			client = ClientFactory.createClient(config);
						
			// create connection
			client.createConnection("node" + node);
		}
		
	}



}
