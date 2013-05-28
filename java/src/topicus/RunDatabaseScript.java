package topicus;

import java.io.File;
import java.util.ArrayList;
import java.util.Observable;
import java.util.Observer;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;

import topicus.databases.AbstractDatabase;
import topicus.databases.GreenplumDatabase;
import topicus.databases.MonetdbDatabase;
import topicus.databases.VerticaDatabase;
import topicus.databases.VoltdbDatabase;
import topicus.databases.CitusdbDatabase;

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
				.isRequired(false)
				.withDescription("Specify the type of database")
				.withLongOpt("type")
				.create("t")
		);
	}
	
	public void run (String[] args) throws Exception {
		super.run(args);
		
		String userHome = System.getProperty("user.home");
		
		File typeFile = new File(userHome + "/benchmark.type");
		
		if (cliArgs.hasOption("type")) {
			type = cliArgs.getOptionValue("type", "");
		} else if (typeFile.exists()) {
			type = FileUtils.readFileToString(typeFile).trim();
		} else {
			throw new Exception("No database type specified through file or argument!");
		}
						
		if (this.validTypes.contains(type) == false) {
			throw new Exception("Invalid database type `" + type + "` specified");
		}
		
		if (type.equals("vertica")) {
			database = new VerticaDatabase();
		} else if(type.equals("voltdb")) {
			database = new VoltdbDatabase();
		} else if (type.equals("citusdb")) {
			database = new CitusdbDatabase();
		} else if (type.equals("greenplum")) {
			database = new GreenplumDatabase();
		} else if (type.equals("monetdb")) {
			database = new MonetdbDatabase();
		} else {
			throw new Exception("Unknown database type specified");
		}
	}
	
	
}
