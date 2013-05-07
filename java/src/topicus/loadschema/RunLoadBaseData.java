package topicus.loadschema;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunDatabaseScript;

public class RunLoadBaseData extends RunDatabaseScript {
	
	public RunLoadBaseData () {
		super();
		validTypes.add("vertica");
		validTypes.add("voltdb");
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the local directory on the database node that contains the base data")
				.withLongOpt("base-data")
				.create()
		);
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		LoadBaseDataScript loader = null;
		
		loader = new LoadBaseDataScript(this.type, this.database);

		
		loader.setCliArgs(cliArgs);
		loader.run();
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		RunLoadBaseData runner = new RunLoadBaseData();
		runner.run(args);
	}

}
