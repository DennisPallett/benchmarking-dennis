package topicus.benchmarking;

import org.apache.commons.cli.OptionBuilder;
import topicus.RunDatabaseScript;


public class RunBenchmarks extends RunDatabaseScript {
	
	public RunBenchmarks () {
		super();
		
		// types we support:
		this.validTypes.add("vertica");

		options.addOption(
				OptionBuilder
				.hasArg()
				.isRequired()
				.withDescription("Specify the CSV file that contains the test queries")
				.withLongOpt("queries")
				.create("q")
		);		
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withType(Number.class)
				.withDescription("Specify the number of nodes to use")
				.withLongOpt("nodes")
				.create("n")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withType(Number.class)
				.withDescription("Specify the number of concurrent users to use")
				.withLongOpt("users")
				.create("u")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withType(Number.class)
				.withDescription("Specify the number of tenants to use")
				.withLongOpt("tenants")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withType(Number.class)
				.withDescription("Specify the number of iterations to perform")
				.withLongOpt("iterations")
				.create("i")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.isRequired(false)
				.withDescription("Whether or not to automatically stop when load/log file already exists")
				.withLongOpt("stop-on-overwrite")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.isRequired(false)
				.withDescription("Automatically overwrite any existing files")
				.withLongOpt("overwrite-existing")
				.create()
		);
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		BenchmarksScript benchmark = null;
		benchmark = new BenchmarksScript(this.type, this.database);
		
		benchmark.setCliArgs(cliArgs);
		benchmark.run();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		RunBenchmarks runner = new RunBenchmarks();
		runner.run(args);
	}

}
