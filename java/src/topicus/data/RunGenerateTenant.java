package topicus.data;

import java.util.ArrayList;

import org.apache.commons.cli.OptionBuilder;

import topicus.RunConsoleScript;
import topicus.RunDatabaseScript;
import topicus.data.GenerateTenantScript.InvalidOutputDirectoryException;

public class RunGenerateTenant extends RunConsoleScript {
	
	public RunGenerateTenant () {
		super();
		
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the tenant template files")
				.withLongOpt("tenant-data-directory")
				.create()
		);	
			
		options.addOption(
				OptionBuilder
				.hasArg()
				.withDescription("Specify the ID of the new tenant")
				.withLongOpt("id")
				.create()
		);
		
		options.addOption(
				OptionBuilder
				.hasArg(false)
				.withDescription("Switch to automatically skip existing tenants")
				.withLongOpt("skip-existing")
				.create()
		);
	}
	
	public void run (String[] args)  throws Exception {
		super.run(args);
		
		GenerateTenantScript script = new GenerateTenantScript();
		
		script.setCliArgs(cliArgs);
		script.run();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		RunGenerateTenant runner = new RunGenerateTenant();
		
		try {
			runner.run(args);
		} catch (InvalidOutputDirectoryException e) {
			System.err.println("ERROR: you must specify a valid output directory with --output!");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
}
