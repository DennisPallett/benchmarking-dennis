package topicus.loadschema;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunDatabaseScript;
import topicus.databases.MonetdbDatabase;

public class RunLoadBaseData extends RunDatabaseScript {
	
	public RunLoadBaseData () {
		super();
		validTypes.add("vertica");
		validTypes.add("voltdb");
		validTypes.add("citusdb");
		validTypes.add("greenplum");
		validTypes.add("monetdb");
		
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
		
		if (type.equals("monetdb")) {
			loader = new LoadBaseDataMonetdbScript(this.type, (MonetdbDatabase) this.database);
		} else {
			loader = new LoadBaseDataScript(this.type, this.database);
		}

		
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
