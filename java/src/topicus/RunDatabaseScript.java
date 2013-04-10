package topicus;

import java.util.ArrayList;

import org.apache.commons.cli.OptionBuilder;

import topicus.databases.AbstractDatabase;
import topicus.databases.VerticaDatabase;

public class RunDatabaseScript extends RunConsoleScript {
	protected final ArrayList<String> validTypes = new ArrayList<String>();
	protected String type = "";
	protected AbstractDatabase database;
	
	public RunDatabaseScript () {
		super();
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the database name")
				.withLongOpt("database")
				.create("d")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the database user")
				.withLongOpt("database-user")
				.create("du")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withType(Number.class)
				.withDescription("Specify the port of the database server")
				.withLongOpt("port")
				.create("p")
		);
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.isRequired()
				.withDescription("Specify the type of database")
				.withLongOpt("type")
				.create("t")
		);
	}
	
	public void run (String[] args) throws Exception {
		super.run(args);
		
		this.type = cliArgs.getOptionValue("type");
		if (this.validTypes.contains(type) == false) {
			throw new Exception("Invalid database type `" + type + "` specified");
		}
		
		if (type.equals("vertica")) {
			database = new VerticaDatabase();
		} else {
			throw new Exception("Unknown database type specified");
		}
	}

}
