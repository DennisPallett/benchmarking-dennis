package topicus.loadschema;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunDatabaseScript;

public class RunLoadSchema extends RunDatabaseScript {
	
	public RunLoadSchema () {
		super();
		this.validTypes.add("vertica");
		
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
		
		LoadSchemaScript loader = null;
		
		if (this.type.equals("vertica")) {
			loader = new LoadSchemaScript(this.type, this.database);
		}
		
		loader.setCliArgs(cliArgs);
		loader.run();
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		RunLoadSchema runner = new RunLoadSchema();
		runner.run(args);
	}

}
