package topicus.loadtenant;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunDatabaseScript;

public class RunUnloadTenant extends RunDatabaseScript {
	
	public RunUnloadTenant () {
		super();
		this.validTypes.add("vertica");
		
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
		
		UnloadTenantScript loader = null;
		
		if (this.type.equals("vertica")) {
			loader = new UnloadTenantScript(this.type, this.database, new VerticaManageTenant());
		}
		
		loader.setCliArgs(cliArgs);
		loader.run();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RunUnloadTenant runner = new RunUnloadTenant();
		
		try {
			runner.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
