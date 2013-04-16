package topicus.benchmarking;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunConsoleScript;

public class RunBenchmarkInstance extends RunConsoleScript {
	
	public RunBenchmarkInstance () {
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the location of the benchmark files")
				.withLongOpt("benchmark-directory")
				.create()
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
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.withDescription("Switch to enable test mode for very fast benchmarks (mainly for development)")
				.withLongOpt("test-mode")
				.create()
		);
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		BenchmarkInstanceScript script = new BenchmarkInstanceScript();
		
		script.setCliArgs(cliArgs);
		script.run();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RunBenchmarkInstance runner = new RunBenchmarkInstance();
		
		try {
			runner.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
