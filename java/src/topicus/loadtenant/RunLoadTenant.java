package topicus.loadtenant;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunDatabaseScript;

public class RunLoadTenant extends RunDatabaseScript {
	
	public RunLoadTenant () {
		super();
		this.validTypes.add("vertica");
		this.validTypes.add("voltdb");
		
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
				.isRequired()
				.withType(Number.class)
				.withDescription("Specify the ID of the tenant to load")
				.withLongOpt("tenant-id")
				.create()
		);	
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.isRequired(false)
				.withDescription("Whether or not to automatically stop when tenant is deployed")
				.withLongOpt("stop-on-deployed")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.isRequired(false)
				.withDescription("Whether or not to automatically stop when load/log file already exists")
				.withLongOpt("stop-on-overwrite")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(true)
				.isRequired(true)
				.withDescription("Specify the file where to save results to")
				.withLongOpt("results-file")
				.create()
		);
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		LoadTenantScript loader = null;
		
		if (this.type.equals("vertica")) {
			loader = new LoadTenantScript(this.type, this.database, new VerticaManageTenant());
		} else {
			loader = new LoadTenantScript(this.type, this.database, new ManageTenant());
		}
		
		loader.setCliArgs(cliArgs);
		loader.run();
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		RunLoadTenant runner = new RunLoadTenant();
		runner.run(args);
	}

}
