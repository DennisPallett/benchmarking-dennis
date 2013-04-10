package topicus;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import topicus.benchmarking.Benchmarks;
import topicus.databases.AbstractDatabase;
import topicus.databases.VerticaDatabase;

public class RunConsoleScript {
	public static final int EXIT_NO_START = 5;
	
	protected CommandLineParser cliParser;
	protected Options options;
	protected CommandLine cliArgs;
	
	protected boolean autoStart = false;
		
	public RunConsoleScript () {
		// create the command line parser
		this.cliParser = new BasicParser();
			
		this.options = new Options();
		
		options.addOption(
				OptionBuilder
				.hasArg(true)
				.withDescription("Autostart this script without confirmation")
				.create("start")
		);		
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.isRequired(false)
				.withDescription("Specify the output directory to store the log file and any results")
				.withLongOpt("output")
				.create("o")
		);
	}
	
	public void run (String[] args)  throws Exception {
		this.cliArgs = this.cliParser.parse(this.options, args);
	}

}
