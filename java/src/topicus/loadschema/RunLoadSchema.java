package topicus.loadschema;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunDatabaseScript;

public class RunLoadSchema extends RunDatabaseScript {
	
	public RunLoadSchema () {
		super();
		validTypes.add("vertica");
		validTypes.add("citusdb");
		validTypes.add("greenplum");
			
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the output directory")
				.withLongOpt("output-directory")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the file that contains the benchmark queries")
				.withLongOpt("queries")
				.create("q")
		);
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		LoadSchemaScript loader = null;
		
		if (this.type.equals("vertica") || this.type.equals("citusdb") || this.type.equals("greenplum")) {
			loader = new LoadSchemaScript(this.type, this.database);
		} else if(this.type.equals("voltdb")) {
			loader = new LoadSchemaVoltdbScript(this.type, this.database);
		} else {
			throw new Exception("Unknown type: " + this.type);
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
