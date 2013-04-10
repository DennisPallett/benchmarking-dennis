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
	 */
	public static void main(String[] args) {
		RunLoadTenant runner = new RunLoadTenant();
		
		try {
			runner.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
