package topicus.cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;

import topicus.ConsoleScript;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.EbsBlockDevice;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class ManageCluster {
	public static final String DEFAULT_NODE_TYPE = "m1.xlarge";
	public static final String DEFAULT_SERVER_TYPE = "m1.medium";
	public static final String DEFAULT_TEST_TYPE = "t1.micro";
	
	public static final String AMI_ID_EBS = "ami-64636a10";
	public static final String AMI_ID = "ami-5a60692e";
	
	public static final String TAG_KEY = "benchmark";
	
	protected PropertiesCredentials credentials;
	protected AmazonEC2Client ec2Client;
	
	protected boolean testMode;
	protected boolean skipClusterCheck;
	
	protected String keyName;
	
	protected boolean infoCached = false;
	
	protected Instance serverInstance;
	protected SortedMap<String, Instance> nodeInstances;
		
	public ManageCluster(File awsFile, String keyName, boolean testMode, boolean skipClusterCheck) throws InvalidCredentialsFileException {
		this.setCredentials(awsFile);
		this.testMode = testMode;
		this.keyName = keyName;
		this.skipClusterCheck = skipClusterCheck;
				
		ec2Client = new AmazonEC2Client(this.credentials);
		ec2Client.setRegion(Region.getRegion(Regions.EU_WEST_1));
	}
	
	public String getDefaultNodeType () {
		if (testMode) {
			return DEFAULT_TEST_TYPE;
		} else {
			return DEFAULT_NODE_TYPE;
		}
	}
	
	public String getDefaultServerType () {
		if (testMode) {
			return DEFAULT_TEST_TYPE;
		} else {
			return DEFAULT_SERVER_TYPE;
		}
	}
	
	public boolean isServerRunning () {
		return (this.serverInstance != null);
	}
	
	public boolean isNodeRunning(int nodeId) {
		return this.nodeInstances.containsKey("node" + nodeId);
	}
	
	public Instance getNode(int nodeId) {
		return nodeInstances.get("node" + nodeId);
	}

	public void startNode(String instanceType) throws InvalidInstanceTypeException, FailedLaunchException {		
		// validate type
		if (instanceType == null || instanceType.length() == 0) {
			instanceType = this.getDefaultNodeType();
		}
		
		if (!validateType(instanceType)) {
			throw new InvalidInstanceTypeException("Invalid instance type `" + instanceType + "` specified");
		}
		
		this._launchInstance(instanceType, "node" + (getNodeCount()+1));
	}
	
	public void stopNode(int nodeId) throws InvalidNodeIdException {
		// don't do anything if node is not running
		if (!isNodeRunning(nodeId)) return;
		
		Instance node = getNode(nodeId);
		
		if (node == null) {
			throw new InvalidNodeIdException();
		}
		
		TerminateInstancesRequest request = new TerminateInstancesRequest();
		ArrayList<String> idList = new ArrayList<String>();
		idList.add(node.getInstanceId());
		request.setInstanceIds(idList);
		
		ec2Client.terminateInstances(request);		
	}
	
	public void startServer(String instanceType) throws InvalidInstanceTypeException, ServerAlreadyRunningException, FailedLaunchException {		
		// validate type
		if (instanceType == null || instanceType.length() == 0) {
			instanceType = this.getDefaultServerType();
		}
		
		if (!validateType(instanceType)) {
			throw new InvalidInstanceTypeException("Invalid instance type `" + instanceType + "` specified");
		}
		
		// check if server is not already running
		if (this.serverInstance != null) {
			throw new ServerAlreadyRunningException("The benchmark server is already running!");
		}
		
		this._launchInstance(instanceType, "benchmarkserver");
	}
	
	public void stopServer() {
		// don't do anything if server is not running
		if (!isServerRunning()) return;

		TerminateInstancesRequest request = new TerminateInstancesRequest();
		ArrayList<String> idList = new ArrayList<String>();
		idList.add(serverInstance.getInstanceId());
		request.setInstanceIds(idList);
		
		ec2Client.terminateInstances(request);		
	}
	
	public void stopAll () {
		this.stopServer();
		
		for(int i=1; i <= this.getNodeCount(); i++) {
			try {
				this.stopNode(i);
			} catch (InvalidNodeIdException e) {
				// don't care, ignoring
			}
		}		
	}
	
	public void setupSsh (ConsoleScript console) throws MissingSshConfigFileException, IOException {
		// find out which nodes and server is active
		String hosts = FileUtils.readFileToString(new File("/etc/hosts"));
		ArrayList<String> activeHosts = new ArrayList<String>();
		
		boolean isActive = true;
		int nodeId = 1;
		while(isActive) {
			if (hosts.indexOf("node" + nodeId) > 0) {
				isActive = true;
				activeHosts.add("node"+ nodeId);
				activeHosts.add(InetAddress.getByName("node" + nodeId).getHostAddress());
			} else {
				isActive = false;
			}
			nodeId++;
		}
		if (hosts.indexOf("benchmarkserver") > 0) {
			activeHosts.add("benchmarkserver");
			activeHosts.add(InetAddress.getByName("benchmarkserver").getHostAddress());
		}
				
		File sshConfFile = new File("/etc/ssh/ssh_config");
		if (sshConfFile.exists() == false) {
			throw new MissingSshConfigFileException("SSH config file (/etc/ssh/ssh_config) does not exist");
		}
		
		String sshConf = FileUtils.readFileToString(sshConfFile);
		
		String toAdd = "IdentityFile ~/ssh_host_rsa_key";
		if (sshConf.indexOf(toAdd) < 0) {
			int beginPos = sshConf.indexOf("Host *");
			sshConf = sshConf.substring(0, beginPos + "Host *".length())
					+ "\n"
					+ toAdd
					+ sshConf.substring(beginPos + "Host *".length());
		}
								
		// write new ssh config file
		FileUtils.writeStringToFile(sshConfFile,  sshConf);
		
		// update sshd config file
		File serverConfFile = new File("/etc/ssh/sshd_config");
		if (serverConfFile.exists() == false) {
			throw new MissingSshConfigFileException("SSH server config file (/etc/ssh/sshd_config) does not exist");
		}
		
		String serverConf = FileUtils.readFileToString(serverConfFile);
		
		// remove any existing AuthorizedKeysFile setting
		serverConf = serverConf.replaceAll("(?m)AuthorizedKeysFile(.*?)$", "");
		serverConf = serverConf.replaceAll("(?m)StrictModes(.*?)$", "");
		serverConf = serverConf.trim();
		serverConf += "\nAuthorizedKeysFile /etc/ssh/global_authorized_keys %h/.ssh/authorized_keys";
		serverConf += "\nStrictModes no";
		
		FileUtils.writeStringToFile(serverConfFile, serverConf);
		
		// restart SSH server
		console.exec("/etc/init.d/ssh restart");
		
		// get all public keys using ssh-keyscan
		HashMap<String, String> keyList = new HashMap<String, String>();
		for(String host : activeHosts) {
			ArrayList<String> commands = new ArrayList<String>();
			commands.add("/bin/bash");
			commands.add("-c");
			commands.add("ssh-keyscan -t rsa " + host);
			
			ProcessBuilder pb = new ProcessBuilder(commands);	
			
			String ret = console.exec(pb.start());
			int sepPos = ret.indexOf("ssh-rsa");
			
			String hostName = ret.substring(0, sepPos).trim();
			String hostKey = ret.substring(sepPos).trim();
			keyList.put(hostName, hostKey);
		}		
		
		// create known_hosts and authorized_keys files
		String knownHosts = "";
		String authorizedKeys = "";
		for(Entry<String, String> entry : keyList.entrySet()) {
			String hostName = entry.getKey();
			String hostKey = entry.getValue();
			
			knownHosts += hostName + " " + hostKey + "\n";
			authorizedKeys += hostKey + "\n";
		}
		
		FileUtils.writeStringToFile(new File("/etc/ssh/ssh_known_hosts"), knownHosts);
		FileUtils.writeStringToFile(new File("/etc/ssh/global_authorized_keys"), authorizedKeys);
	}
	
	public String updateHostsFile () throws IOException {
		boolean usePublic = true;
		try {
			HttpClient client = new DefaultHttpClient();
			HttpParams params = client.getParams();
			HttpConnectionParams.setConnectionTimeout(params, 2000);
			HttpConnectionParams.setSoTimeout(params, 2000);		
			HttpGet httpGet = new HttpGet("http://169.254.169.254/latest/meta-data/instance-id");
			HttpResponse response = client.execute(httpGet);
		
			String instanceId = EntityUtils.toString(response.getEntity());
			usePublic = false;
		} catch (ConnectTimeoutException e) {
			usePublic = true;
		}
		
		return this.updateHostsFile(usePublic);
	}
	
	public String updateHostsFile (boolean usePublic) throws IOException {
		List<File> files = new ArrayList<File>();
		files.add(new File("C:\\Windows\\system32\\drivers\\etc\\hosts"));
		files.add(new File("/etc/hosts"));
				
		// do nothing if nothing is running
		if (this.getNodeCount() == 0 && !this.isServerRunning()) return "";
		
		for(File file : files) {
			if (!file.exists()) continue;
			
			String hosts = FileUtils.readFileToString(file);

			// update hosts file for nodes
			for(Map.Entry<String, Instance> entry : nodeInstances.entrySet()) {
				String nodeName = entry.getKey();
				Instance node = entry.getValue();
				
				String ip = (usePublic) ? node.getPublicIpAddress() : node.getPrivateIpAddress();
				hosts = _replaceHostsEntry(hosts, nodeName, ip);				
			}
			
			// update hosts file for server
			if (isServerRunning()) {
				String ip = (usePublic) ? serverInstance.getPublicIpAddress() : serverInstance.getPrivateIpAddress();
				hosts = _replaceHostsEntry(hosts, "benchmarkserver", ip);
			}
			
			FileUtils.writeStringToFile(file, hosts);
		}
		
		return (usePublic) ? "public" : "private";
	}

	protected String _replaceHostsEntry(String hosts, String hostName, String newIp) {
		// remove existing entry
		hosts = hosts.trim();
		hosts = hosts.replaceAll("(.*)(\\s+)" + hostName, "");
		hosts = hosts.trim();
		
		// add new entry
		hosts += "\n";
		hosts += newIp + "\t" + hostName;
		
		return hosts;
	}
	
	
	public Map<String, Instance> getNodes () {
		return this.nodeInstances;
	}
	
	public int getNodeCount () {
		return this.nodeInstances.size();
	}
	
	public Instance getServerInstance () {
		this.updateClusterInfo();
		
		return this.serverInstance;
	}

	public void setCredentials(File awsFile) throws InvalidCredentialsFileException {
		try {
			this.credentials = new PropertiesCredentials(awsFile);
		} catch (Exception e) {
			throw new InvalidCredentialsFileException("Invalid AWS credentials file specified!");
		}		
	}
	
	public boolean validateType(String type) {
		type = type.toLowerCase();
		return (type.equals("t1.micro") || type.equals("m1.medium") || type.equals("m1.large") || type.equals("m1.xlarge"));
	}
	
	public void updateClusterInfo () {
		this.updateClusterInfo(false);
	}
	
	public void updateClusterInfo (boolean refreshCache) {
		if (infoCached && !refreshCache) return;
		
		this.serverInstance = null;
		this.nodeInstances = new TreeMap<String, Instance>();
		
		// setup filters
		ArrayList<Filter> filters = new ArrayList<Filter>();		
		String[] values = {TAG_KEY};
		filters.add(new Filter("tag-key", Arrays.asList(values)));
		
		ArrayList<String> states = new ArrayList<String>();
		states.add("pending");
		states.add("running");
		filters.add(new Filter("instance-state-name", states));
		
		// setup request (with filters)
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		request.withFilters(filters);
		
		// get matching instances
		DescribeInstancesResult result = ec2Client.describeInstances(request);
		
		// loop through reservations
		List<Reservation> resList = result.getReservations();		
		for (Reservation res : resList) {
			// loop through instances
			List<Instance> instanceList = res.getInstances();
			for (Instance instance : instanceList) {
				// get tags of instance to determine if it's a node or the server
				List<Tag> tags = instance.getTags();
				Tag tag = tags.get(0);
				
				if (tag.getValue().equals("benchmarkserver")) {
					this.serverInstance = instance;
				} else {
					this.nodeInstances.put(tag.getValue(), instance);
				}											
			}
		}
		
		infoCached = true;
	}
	
	protected void _launchInstance(String instanceType, String name) throws FailedLaunchException {
		// start new run request
		RunInstancesRequest request = new RunInstancesRequest();
		
		// launch exactly 1 instance
		request.withMaxCount(1);
		request.withMinCount(1);
		
		// specify key-pair
		request.withKeyName(this.keyName);
		
		// specify start-up script
		// which generates AWS credentials file
		request.withUserData(Base64.encodeBase64String(this._setupAwsScript(name).getBytes()));
				
		// set instance type
		request.withInstanceType(instanceType);
		
		// set image 
		if (instanceType.equals("t1.micro")) {
			request.withImageId(AMI_ID_EBS);
		} else {
			request.withImageId(AMI_ID);
		}
		
		// setup block devices
		ArrayList<BlockDeviceMapping> blockList = new ArrayList<BlockDeviceMapping>();
				
		if (instanceType.equals("t1.micro")) {
			BlockDeviceMapping block= new BlockDeviceMapping();
			block.setDeviceName("/dev/sda1");
			
			EbsBlockDevice ebs = new EbsBlockDevice();
			ebs.setDeleteOnTermination(true);
			ebs.setVolumeSize(20);
			
			block.setEbs(ebs);
			
			blockList.add(block);
		}
		
		// add instance storage 1
		if (instanceType.equals("m1.medium") || instanceType.equals("m1.large") || instanceType.equals("m1.xlarge")) {
			BlockDeviceMapping block = new BlockDeviceMapping();
			block.setDeviceName("/dev/sdb");
			block.setVirtualName("ephemeral0");
			
			blockList.add(block);		
		}
		
		// add instance storage 2
		if (instanceType.equals("m1.large") || instanceType.equals("m1.xlarge")) {
			BlockDeviceMapping block = new BlockDeviceMapping();
			block.setDeviceName("/dev/sdc");
			block.setVirtualName("ephemeral1");
			
			blockList.add(block);		
		}
		
		// add instance storage 3 and 4
		if (instanceType.equals("m1.xlarge")) {
			BlockDeviceMapping block1 = new BlockDeviceMapping();
			block1.setDeviceName("/dev/sdd");
			block1.setVirtualName("ephemeral2");
			
			BlockDeviceMapping block2 = new BlockDeviceMapping();
			block2.setDeviceName("/dev/sde");
			block2.setVirtualName("ephemeral3");
			
			blockList.add(block1);		
			blockList.add(block2);
		}
		
		request.withBlockDeviceMappings(blockList);
		
		// launch instance
		RunInstancesResult runInstances = ec2Client.runInstances(request);
		
		List<Instance> instances = runInstances.getReservation().getInstances();
		
		if (instances.size() != 1) {
			throw new FailedLaunchException("Instance failed to launch");
		}
		
		Instance instance = instances.get(0);
		
		CreateTagsRequest tagRequest = new CreateTagsRequest();
		tagRequest.withResources(instance.getInstanceId());
		tagRequest.withTags(new Tag(TAG_KEY, name));		
		
		ec2Client.createTags(tagRequest);
	}
	
	protected String _setupAwsScript (String name) {
		StringBuilder script = new StringBuilder();
		
		script.append("#!/bin/bash");
		script.append("\n");
		script.append("set -e -x");
		script.append("\n");
		script.append("export DEBIAN_FRONTEND=noninteractive");
		script.append("\n");
				
		String fileName = "AwsCredentials.properties";
		script.append("echo \"accessKey=" + this.credentials.getAWSAccessKeyId() + "\" >> ~/" + fileName);
		script.append("\n");
		script.append("echo \"secretKey=" + this.credentials.getAWSSecretKey() + "\" >> ~/" + fileName);	
		
		script.append("\n");
		script.append("hostname " + name);
		
		return script.toString();
	}
	
	public class InvalidCredentialsFileException extends Exception {
		public InvalidCredentialsFileException(String string) {
			super(string);
		}
	}
	
	public class InvalidInstanceTypeException extends Exception {
		public InvalidInstanceTypeException(String string) {
			super(string);
		}
	}
	
	public class FailedLaunchException extends Exception {
		public FailedLaunchException(String string) {
			super(string);
		}
	}
	
	public class ServerAlreadyRunningException extends Exception {
		public ServerAlreadyRunningException(String string) {
			super(string);
		}
	}
	
	public class InvalidNodeIdException extends Exception {
		
	}
	
	public class MissingSshConfigFileException extends Exception {
		public MissingSshConfigFileException(String string) {
			super(string);
		}
	}

}
