package topicus.databases;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
	protected Session sshSession = null;
	protected String sshError = "";
	
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
	
	public Session getSshSession () {
		if (this.sshSession == null) {
			JSch jsch = new JSch();
			try {
				jsch.addIdentity(System.getProperty("user.home") + "/ssh_host_rsa_key");
				Session session = jsch.getSession("root", "node1");
				session.setConfig("StrictHostKeyChecking", "no");
				
				session.connect();
				
				this.sshSession = session;
			} catch (JSchException e) {
				// do nothing, ignore
				sshError = e.getMessage();
			}			
		}
		
		return this.sshSession;
	}
	
	public void close () {
		if (this.sshSession != null) {
			this.sshSession.disconnect();
		}
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
	
	public int[] deployData(Connection conn, String fileName, String tableName)
			throws SQLException {
		
		int rows = -1;
		int runTime = -1;
		
		// get SSH connection
		Session session = this.getSshSession();
		
		// check if we have SSH connection
		if (session == null) {
			throw new SQLException("Unable to setup SSH connection with node1: " + sshError);
		}
		
		// open up channel to execute csvloader command
		Channel channel;
		try {
			channel = session.openChannel("exec");
		} catch (JSchException e) {
			throw new SQLException(e.getMessage());
		}
		
		// the CSVLoader command to load the data
		String command = "csvloader \"" + tableName + "\" -f \"" + fileName + "\" --separator \"#\" --blank  null";
		((ChannelExec)channel).setCommand(command);
		
		channel.setInputStream(null);
		((ChannelExec)channel).setErrStream(System.err);
		
		// get output stream
		InputStream in = null;
		try {
			in = channel.getInputStream();
		} catch (IOException e) {
			throw new SQLException(e.getMessage());
		}
		
		// execute command
		try {
			channel.connect();
		} catch (JSchException e) {
			throw new SQLException(e.getMessage());
		}
		
		// stream output
		Pattern pRows = Pattern.compile("Inserted (\\d+)");
		Pattern pRunTime = Pattern.compile("elapsed: (\\d+\\.\\d+) seconds");
		
		try {
			byte[] tmp=new byte[1024];
			while (true) {
				while(in.available() > 0){
					int i = in.read(tmp, 0, 1024);
					if(i<0)break;
					
					String output = new String(tmp, 0, i);
					printLine(output);
					
					Matcher mRows = pRows.matcher(output);
					if (mRows.find()) {
						rows = Integer.parseInt(mRows.group(1));
					}
					
					Matcher mRunTime = pRunTime.matcher(output);
					if (mRunTime.find()) {
						// get runtime in seconds and *1000 to get milliseconds
						runTime = (int) (Float.parseFloat(mRunTime.group(1))*1000);
					}					
		        }
				
		        if(channel.isClosed()){
		        	break;
		        }
		        
		        try{Thread.sleep(1000);}catch(Exception ee){}
			}
		} catch (IOException e) {
			channel.disconnect();
			throw new SQLException(e.getMessage());
		}
		
		// close channel
		channel.disconnect();	      
		
		return new int[]{runTime, rows};
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
				String yearKey = setInfo[1];
				String parentId = setInfo[2];
				
				long startTime = System.currentTimeMillis();
				ClientResponse response = null;
				boolean timeout = false;
				boolean failed = false;
				
				try {
					response = client.callProcedure(procName,  yearKey, parentId);
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
