package topicus.results;

import java.util.ArrayList;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunConsoleScript;
import topicus.RunDatabaseScript;

public class RunProcessResults extends RunConsoleScript {
	
	public RunProcessResults () {
		super();
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the directory where the result files are located")
				.withLongOpt("results-directory")
				.create()
		);	
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the MySQL user")
				.withLongOpt("user")
				.create("u")
		);	
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the password for the MySQL user")
				.withLongOpt("password")
				.create("p")
		);	
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the database where the results should be stored")
				.withLongOpt("database")
				.create("d")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the database host or IP address")
				.withLongOpt("host")
				.create("h")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify an alias for the product type in the filename")
				.withLongOpt("alias")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.withDescription("Enable automatic overwriting of old results in database")
				.withLongOpt("overwrite")
				.create()
		);
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		ProcessResultsScript script = new ProcessResultsScript();
		
		script.setCliArgs(cliArgs);
		script.run();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RunProcessResults runner = new RunProcessResults();
		
		try {
			runner.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
}
