package topicus.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import topicus.ConsoleScript;

public class GenerateTenantScript extends ConsoleScript {
	class InvalidOutputDirectoryException extends Exception {}
	class InvalidDataDirectoryException extends Exception {}
	class InvalidTenantIdException extends Exception {}
	class CancelledException extends Exception {}
	
	protected String outputDirectory;
	protected int tenantId;
	protected String dataDirectory;
	protected String tenantDirectory;
	
	protected HashMap<String, Integer> rowCounts = new HashMap<String, Integer>();
	
	public void run () throws Exception {
		printLine("Started-up tenant generator tool");	
		
		this._setOptions();
		
		if (!this.cliArgs.hasOption("start")) {
			if (!confirmBoolean("Start generating data for tenant #" + tenantId + "? (y/n)")) {
				printError("Stopping");
				throw new CancelledException();
			}
		}
		
		this._checkTenantExists();
		
		this._createTenantFile("adm_data.tbl");
		this._createTenantFile("org_data.tbl");
		this._createTenantFile("closure_org_data.tbl");
		this._createTenantFile("gb_data.tbl");
		this._createTenantFile("kp_data.tbl");
		
		this.printLine("Successfully finished!");
	}
	
	protected void _createTenantFile(String fileName) throws Exception {
		printLine("Creating " + fileName);
		
		File file = new File(this.dataDirectory + fileName);
		if (!file.exists()) {
			printError("Missing data file `" + fileName + "` in data directory!");
			throw new Exception();
		}
		
		// load data in memory
		ArrayList<String> lines = new ArrayList<String>();
		Scanner scan = new Scanner(file).useDelimiter("\n");
		while(scan.hasNext()) {
			lines.add(scan.next());
		}
		
		printLine("File in memory");
		
		// store row count for later use with fact table
		int rowCount = lines.size();
		this.rowCounts.put(fileName,  rowCount);
		
		//String newFile = "";
		StringBuilder newFile = new StringBuilder((int)file.length());
		for (String line : lines) {
			StringBuilder newLine = new StringBuilder(line.trim());
			//String newLine = line;
			//newLine = newLine.trim();
			
			// replace all keys with tenant-specific keys
			int pkPos;
			while((pkPos = newLine.indexOf("PK:")) > -1) {
				int endPos = newLine.indexOf("#", pkPos);
				if (endPos < 1) {
					endPos = newLine.length()-1;
				}
								
				// get PK value
				String pk = newLine.substring(pkPos+3, endPos);
				
				// determine new PK value
				String newPk = "";
				if (pk.toLowerCase().equals("null")) {
					newPk = "NULL";
				} else if (pk.equals("0") && fileName == "org_data.tbl") {
					newPk = String.valueOf(this.tenantId);
				} else {
					newPk = String.valueOf( ((this.tenantId-1) * rowCount) + Integer.parseInt(pk) );
				}
				
				// replace PK
				newLine.replace(pkPos, endPos, newPk);
			}
			
			/* TODO:
			if ($file == 'closure_org_data.tbl') {
				$newLine = $this->_replaceFK($newLine, 'PK_ORG:', $this->rowCounts['org_data.tbl']);
				$newLine = $this->_replaceFK($newLine, 'PK_ORG:', $this->rowCounts['org_data.tbl']);
			}
			*/

			// add new line
			newLine.append("\n");

			// add line to file
			newFile.append(newLine);
		}
		
		// add in new tenant ID
		String newContent = newFile.toString();
		newContent = newContent.replace("%TENANT_ID%", String.valueOf(this.tenantId));
		
		// write new file
		FileUtils.writeStringToFile(new File(this.tenantDirectory + fileName), newContent);
		
		printLine("Written `" + fileName + "` to tenant directory");		
	}
	
	protected void _checkTenantExists() throws Exception {
		File tenantDir = new File(this.tenantDirectory);
		if (tenantDir.exists()) {
			printError("Tenant directory already exists");
			if (!confirmBoolean("Do you want to delete existing tenant directory? (y/n)")) {
				printError("Stopping");
				throw new CancelledException();
			}
			
			FileUtils.deleteDirectory(tenantDir);
		}
		
		// create tenant dir
		tenantDir.mkdir();
		this.printLine("Created tenant directory");
	}
	
	protected void _setOptions () throws Exception {
		// output directory
		this.outputDirectory = cliArgs.getOptionValue("output", "");
		File outputDir = new File(this.outputDirectory, "");
		if (outputDir.exists() == false || outputDir.isFile()) {
			throw new InvalidOutputDirectoryException();
		}
		this.outputDirectory = outputDir.getAbsolutePath() + "/";
		this.printLine("Output directory: " + this.outputDirectory);
		
		// output directory
		this.dataDirectory = cliArgs.getOptionValue("tenant-data-directory", "");
		File dataDir = new File(this.dataDirectory, "");
		if (dataDir.exists() == false || dataDir.isFile()) {
			throw new InvalidDataDirectoryException();
		}
		this.dataDirectory = dataDir.getAbsolutePath() + "/";
		this.printLine("Using data directory: " + this.dataDirectory);
		
		this.tenantId = Integer.parseInt(cliArgs.getOptionValue("id"));
		this.printLine("Tenant ID set to: " + this.tenantId);
		
		if (this.tenantId > 700) {
			this.printError("Invalid tenant ID spciefied, only support up to 700");
			throw new InvalidTenantIdException ();
		}
		
		this.tenantDirectory = this.outputDirectory + this.tenantId + "/";
	}
	
}
