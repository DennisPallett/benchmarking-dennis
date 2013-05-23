package topicus.benchmarking;

import java.util.ArrayList;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunConsoleScript;
import topicus.RunDatabaseScript;

public class RunAllBenchmarks extends RunDatabaseScript {
	
	public RunAllBenchmarks () {
		super();
		
		// types we support:
		this.validTypes.add("vertica");
		this.validTypes.add("voltdb");
		this.validTypes.add("greenplum");
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.isRequired()
				.withDescription("Specify the local directory on the database node that contains the tenant data")
				.withLongOpt("tenant-data")
				.create()
		);	
		
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
				.withDescription("Specify the number of iterations to perform")
				.withLongOpt("iterations")
				.create("i")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the S3 bucket to upload results to")
				.withLongOpt("bucket")
				.create()
		);	
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the AWS credentials file if using S3")
				.withLongOpt("aws-credentials")
				.create()
		);	
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		AllBenchmarksScript benchmark = new AllBenchmarksScript(this.type, this.database);
		
		benchmark.setCliArgs(cliArgs);
		benchmark.run();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RunAllBenchmarks runner = new RunAllBenchmarks();
		
		try {
			runner.run(args);
		} catch (AllBenchmarksScript.CancelledException e) {
			System.err.println("Script has been cancelled by user!");
			
			// making sure we really stop!
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
}
