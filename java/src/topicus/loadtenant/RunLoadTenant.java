package topicus.loadtenant;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunDatabaseScript;

public class RunLoadTenant extends RunDatabaseScript {
	
	public RunLoadTenant () {
		super();
		this.validTypes.add("vertica");
		
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
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		LoadTenantScript loader = null;
		
		if (this.type.equals("vertica")) {
			loader = new LoadTenantScript(this.type, this.database, new VerticaManageTenant());
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
