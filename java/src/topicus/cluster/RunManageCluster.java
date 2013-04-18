package topicus.cluster;

import java.util.ArrayList;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunConsoleScript;
import topicus.RunDatabaseScript;

public class RunManageCluster extends RunConsoleScript {
	
	public RunManageCluster () {
		super();
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the action to execute")
				.withLongOpt("action")
				.create("a")
		);	
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the AWS credentials file")
				.withLongOpt("aws-credentials")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the EC2 instance type to use")
				.withLongOpt("type")
				.create("t")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the name of the key-pair to use")
				.withLongOpt("key")
				.create("k")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.withDescription("Enable test mode (only use micro instances)")
				.withLongOpt("test-mode")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.withDescription("Sets the hosts file to use public IP's")
				.withLongOpt("use-public-ip")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withType(Number.class)
				.withDescription("Specify the node ID")
				.withLongOpt("node")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.withDescription("Switch to skip cluster status check")
				.withLongOpt("skip-cluster-check")
				.create()
		);
		

	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		ManageClusterScript script = new ManageClusterScript();
		
		script.setCliArgs(cliArgs);
		script.run();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RunManageCluster runner = new RunManageCluster();
		
		try {
			runner.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
}
