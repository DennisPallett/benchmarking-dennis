package topicus.cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
	
	protected String keyName;
	
	protected boolean infoCached = false;
	
	protected Instance serverInstance;
	protected SortedMap<String, Instance> nodeInstances;
		
	public ManageCluster(File awsFile, String keyName, boolean testMode) throws InvalidCredentialsFileException {
		this.setCredentials(awsFile);
		this.testMode = testMode;
		this.keyName = keyName;
		
		this.nodeInstances = new TreeMap<String, Instance>();
		
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
	
	public void stopNode(int nodeId) throws Exception {
		// don't do anything if node is not running
		if (!isNodeRunning(nodeId)) return;
		
		Instance node = getNode(nodeId);
		
		if (node == null) {
			throw new Exception("Instance node is null!");
		}
		
		TerminateInstancesRequest request = new TerminateInstancesRequest();
		ArrayList<String> idList = new ArrayList<String>();
		idList.add(node.getInstanceId());
		request.setInstanceIds(idList);
		
		ec2Client.terminateInstances(request);		
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
				
				if (tag.getValue().equals("server")) {
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

}
