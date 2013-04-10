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
				.withType(Number.class)
				.withDescription("Specify the number of nodes to use")
				.withLongOpt("nodes")
				.create("n")
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
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
}
