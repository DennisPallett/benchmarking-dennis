package topicus.cluster;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import com.amazonaws.services.ec2.model.Instance;

import topicus.ConsoleScript;
import topicus.cluster.ManageCluster.InvalidCredentialsFileException;
import topicus.cluster.ManageCluster.InvalidNodeIdException;
import topicus.cluster.ManageCluster.MissingSshConfigFileException;
import topicus.cluster.ManageCluster.MissingTenantEbsException;
import topicus.cluster.ManageCluster.TenantEbsUnavailableException;

public class ManageClusterScript extends ConsoleScript {
	protected String awsCredentialsFile;
	
	protected ManageCluster manageCluster;
	
	protected boolean testMode = false;
	protected boolean skipClusterCheck = false;
	
	protected String keyName = "";
	
	protected String type = "";
	
	public void run () throws Exception {
		printLine("Started-up cluster management tool");	
		
		this._setOptions();
		
		if (!skipClusterCheck) {
			printLine("Loading information about cluster");
			this.manageCluster.updateClusterInfo();
			printLine("Information loaded");
		}
		
		// determine action
		String action = this.cliArgs.getOptionValue("action", "").toLowerCase();
		if (action.equals("start-node")) {
			this.startNode();
		} else if (action.equals("stop-node")) {
			this.stopNode();
		} else if (action.equals("start-server")) {
			this.startServer();
		} else if (action.equals("stop-server")) {
			this.stopServer();
		} else if (action.equals("stop-all")) {
			this.stopAll();
		} else if (action.equals("status")) {
			this.printStatus();
		} else if (action.equals("update-hosts")) {
			this.updateHosts();
		} else if (action.equals("run-gui")) {
			this.runGUI();
		} else if (action.equals("setup-ssh")) {
			this.setupSsh();
		} else if (action.equals("attach-tenant-ebs")) {
			this.attachTenantEbs();
		} else if (action.equals("detach-tenant-ebs")) {
			this.detachTenantEbs();
		} else {
			throw new InvalidActionException("The specified action `" + action + "` is not a valid action");
		}
		
		this.printLine("Successfully finished!");
	}
	
	protected void attachTenantEbs () throws CancelledException, InvalidNodeSpecifiedException, 
		MissingTenantEbsException, TenantEbsUnavailableException {
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to attach tenant data EBS? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		int nodeId = Integer.parseInt(cliArgs.getOptionValue("node", "0"));
		if (nodeId < 1) {
			throw new InvalidNodeSpecifiedException("Invalid node ID specified");
		}
		
		// check if node is running
		if (!manageCluster.isNodeRunning(nodeId)) {
			throw new InvalidNodeSpecifiedException("Node #" + nodeId + " is not running");
		}
		
		
		printLine("Attaching tenant data EBS to node #" + nodeId);
		try {
			manageCluster.attachTenantEbs(nodeId);
			printLine("Tenant EBS has been attached!");
			printLine("Use `mount /dev/xvdf /tenant-data` to mount volume in instance");
		} catch (InvalidNodeIdException e) {
			throw new InvalidNodeSpecifiedException("Node #" + nodeId + " is not running");
		}	
	}
	
	protected void detachTenantEbs () throws CancelledException, MissingTenantEbsException {
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to dettach tenant data EBS? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		printLine("Dettaching tenant data EBS");

		
		try {
			manageCluster.detachTenantEbs();
			printLine("Tenant EBS has been detached!");
		} catch (MissingTenantEbsException e) {
			throw e;
		}
	}
	
	protected void setupSsh () throws CancelledException, IOException, MissingSshConfigFileException {
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to setup SSH configuration? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		printLine("Setting up SSH configuration");
		manageCluster.setupSsh(this);
		printLine("SSH configuration set up!");
	}
	
	protected void printStatus () throws Exception {
		printLine("Current status of benchmark cluster");
		printLine("===================================");
		
		Instance server = manageCluster.getServerInstance();
		if (server == null) {		
			printLine("Server: not running");
		} else {
			printInstanceInfo("Server", server);
		}
		
		printLine("==================");
		printLine("Nodes:");
		
		if (manageCluster.getNodeCount() == 0) {
			printLine("-none-");
		} else {
			Map<String, Instance> nodes = manageCluster.getNodes();
			for(Map.Entry<String, Instance> entry : nodes.entrySet()) {
				String nodeName = entry.getKey();
				Instance nodeInstance = entry.getValue();
				printInstanceInfo(nodeName, nodeInstance);				
			}
		}		
		printLine("===================================");		
	}
	
	protected void printInstanceInfo(String name, Instance instance) {
		// calculate how many instance hours have been used
		Date dateNow = new Date();
		double diff = dateNow.getTime() - instance.getLaunchTime().getTime();
		double upHours = Math.ceil(((diff / 1000) / 60) / 60);
		
		printLine("- " + name + ": "
				+ "\t" + instance.getState().getName()
				+ "\t" + instance.getPublicDnsName()
				+ "\t" + instance.getInstanceType()
				+ "\t" + instance.getLaunchTime()
				+ "\t" + upHours + " hours");
		
	}
	
	protected void stopNode () throws Exception {
		this.printLine("Ready to stop a node");
		
		int nodeId = Integer.parseInt(cliArgs.getOptionValue("node", "0"));
		if (nodeId < 1) {
			throw new InvalidNodeSpecifiedException("Invalid node ID specified");
		}
		
		// check if node is running
		if (!manageCluster.isNodeRunning(nodeId)) {
			throw new InvalidNodeSpecifiedException("Node #" + nodeId + " is not running");
		}
		
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to stop node #" + nodeId + "? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		manageCluster.stopNode(nodeId);
		
		printLine("Node #" + nodeId + " has been stopped");
	}
	
	protected void startNode() throws Exception {
		this.printLine("Ready to start a new node");
		
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to start a new node? (y/n)")) {
				throw new CancelledException("Stopped by user");
			}
		}
				
		// type specified or not?
		if (this.type.length() == 0) {
			this.type = this.confirm("Specify the instance type you want to use for the new node: " +
					"(leave blank to use default `" + manageCluster.getDefaultNodeType() + "`` type) ");
		}	
		
		if (type.length() == 0) {
			type = manageCluster.getDefaultNodeType();
		}
		printLine("Instance type set to: " + this.type);
		
		manageCluster.startNode(this.type);
		
		printLine("Node has been launched and is starting up");
	}
	
	protected void startServer() throws Exception {
		this.printLine("Ready to start benchmark server");
			
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to start the benchmark server? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
				
		// type specified or not?
		if (this.type.length() == 0) {
			this.type = this.confirm("Specify the instance type you want to use for the new node: " +
					"(leave blank to use default `" + manageCluster.getDefaultServerType() + "`` type) ");
		}	
		
		if (type.length() == 0) {
			type = manageCluster.getDefaultServerType();
		}
		printLine("Instance type set to: " + this.type);
		
		manageCluster.startServer(this.type);
		
		printLine("Server has been launched and is starting up");
	}
	
	protected void stopServer () throws Exception {
		this.printLine("Ready to stop the benchmark server");
			
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to stop the benchmark server? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		manageCluster.stopServer();
		
		printLine("The benchmark server has been stopped");
	}
	
	protected void stopAll () throws Exception {
		this.printLine("Ready to stop all benchmark instances");
			
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to stop all benchmark instances? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		manageCluster.stopAll();
		
		printLine("All benchmark instances have been stopped");
	}
	
	protected void updateHosts() throws Exception {
		this.printLine("Ready to update hosts file");
		
		if (!this.cliArgs.hasOption("start")) {
			if (!this.confirmBoolean("Are you sure you want to update the hosts file? (y/n)")) {
				throw new CancelledException("Cancelled by user");
			}
		}
		
		printLine("Updating hosts file...");
		
		String ret = "";
		if (this.cliArgs.hasOption("use-public-ip")) {
			ret = manageCluster.updateHostsFile(true);
		} else {
			ret = manageCluster.updateHostsFile();
		}
		
		if (ret.equals("public")) {
			printLine("The hosts file has been updated with public IP addresses");
		} else if (ret.equals("private")) {
			printLine("The hosts file has been updated with private IP addresses");
		} else {
			printLine("The host file has not been updated because nothing is running");
		}
	}
	
	protected void runGUI () {
		printLine("Launching GUI");
		new ManageClusterGUI(manageCluster).setVisible(true);
		printLine("GUI running");
	}
	
	protected void _setOptions () throws MissingAwsCredentialsException, InvalidCredentialsFileException {
		this.awsCredentialsFile = cliArgs.getOptionValue("aws-credentials", System.getProperty("user.home") + "/AwsCredentials.properties");
		printLine("Using AWS credentials: " + this.awsCredentialsFile);
		File awsFile = new File(this.awsCredentialsFile);
		if (awsFile.exists() == false) {
			throw new MissingAwsCredentialsException("Missing AWS credentials file");
		}
		
		this.testMode  = this.cliArgs.hasOption("test-mode");
		
		this.type = cliArgs.getOptionValue("type", "").toLowerCase();
		
		this.keyName = cliArgs.getOptionValue("key", "dennis");
		
		this.skipClusterCheck = cliArgs.hasOption("skip-cluster-check");
		
		printLine("Setting up client");
		this.manageCluster = new ManageCluster(awsFile, this.keyName, this.testMode, skipClusterCheck);
		printLine("Client setup");
	}
	
	public class InvalidActionException extends Exception {
		public InvalidActionException(String string) {
			super(string);
		}
	}
	
	public class CancelledException extends Exception {
		public CancelledException(String string) {
			super(string);
		}
	}
	
	public class MissingAwsCredentialsException extends Exception {
		public MissingAwsCredentialsException(String string) {
			super(string);
		}
	}
	
	public class InvalidNodeSpecifiedException extends Exception {
		public InvalidNodeSpecifiedException(String string) {
			super(string);
		}
	}
}
