package topicus.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
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
	
	protected Random randomGenerator;
	
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
		
		this._createTenantFactFile();
		
		this.printLine("Successfully finished!");
	}
	
	protected void _createTenantFactFile () throws Exception {
		printLine("Generating fact file");
		
		File file = new File(this.dataDirectory + "fe_data.tbl");
		if (!file.exists()) {
			printError("Missing data file `fe_data.tbl` in data directory!");
			throw new Exception();
		}
		
		// load data in memory
		this.printLine("Loading fact file into memory");
		String[] lines = new String[3740431];
		Scanner scan = new Scanner(file).useDelimiter("\n");
		int i = 0;
		while(scan.hasNext()) {
			lines[i] = scan.next();
			i++;
		}
		
		int rowCount = lines.length;
		
		printLine("File in memory, found " + rowCount + " rows");
				
		int primaryKey = ((this.tenantId-1) * rowCount * 4) + 1;
		int lineCounter = 1;
		
		File newFile = new File(this.tenantDirectory + "fe_data.tbl");
		double randFloat = randomGenerator.nextInt(500) / 100;
		StringBuilder stringBuffer = new StringBuilder((int)file.length()/3);
		for(int j=0; j < 4; j++) {
			for (String line : lines) {
				StringBuilder newLine = new StringBuilder();
				
				newLine.append(primaryKey);
				newLine.append("#");
				newLine.append(tenantId);
				newLine.append("#");
				newLine.append(line.trim());
				newLine.append("\n");		
				
				// replace FK's
				this._replaceFK(newLine,  "PK_ORG:",  rowCounts.get("org_data.tbl"));
				this._replaceFK(newLine,  "PK_ADMIN:",  rowCounts.get("adm_data.tbl"));
				this._replaceFK(newLine,  "PK_KP:",  rowCounts.get("kp_data.tbl"));
				this._replaceFK(newLine,  "PK_GB:",  rowCounts.get("gb_data.tbl"));
				
				int startPos = newLine.indexOf("VALUE:");
				if (startPos > -1) {
					int endPos = newLine.indexOf("#", startPos);
					
					// retrieve value and multiply by random multiplier
					double value = Float.parseFloat(newLine.substring(startPos+("VALUE:").length(), endPos));
					value = value * randFloat;
					
					// insert new value into line
					newLine.replace(startPos, endPos,  String.valueOf(value));
				}
				
				stringBuffer.append(newLine);
				
				if (lineCounter % 100000 == 0) {
					printLine("Processed " + lineCounter + " rows");
					randFloat = randomGenerator.nextInt(500) / 100;
				}
				
				if (lineCounter % 2000000 == 0) {
					printLine("Writing lines");
					FileUtils.writeStringToFile(newFile, stringBuffer.toString(), true);
					stringBuffer = new StringBuilder((int)file.length()/3);
				}
				
				lineCounter++;
				primaryKey++;
			}
			
			
		}
		
		FileUtils.writeStringToFile(newFile, stringBuffer.toString(), true);
		printLine("Processed " + lineCounter + " rows");
		
		lines = null;
		stringBuffer = null;
		printLine("Finished fact file!");
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
		
		StringBuilder newFile = new StringBuilder((int)file.length());
		for (String line : lines) {
			StringBuilder newLine = new StringBuilder(line.trim());
			
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
			
			if (fileName.equals("closure_org_data.tbl")) {
				this._replaceFK(newLine, "PK_ORG:", this.rowCounts.get("org_data.tbl"));
				this._replaceFK(newLine, "PK_ORG:", this.rowCounts.get("org_data.tbl"));
			}
			
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
	
	protected void _replaceFK(StringBuilder line, String keyName, int rowCount) {
		int startPos = line.indexOf(keyName);
		int endPos = line.indexOf("#", startPos);
		if (endPos < startPos) {
			endPos = line.length();
		}
		
		String value = line.substring(startPos+keyName.length(), endPos);
			
		String newValue = "";
		if (value.toLowerCase().equals("null") && keyName.equals("PK_ORG:")) {
			newValue = "0";
		} else if (value.equals("0") && keyName.equals("PK_ORG:")) {
			newValue = String.valueOf(this.tenantId);
		} else if (value.toLowerCase().equals("null") == false) {
			newValue = String.valueOf( ((this.tenantId-1) * rowCount) + Integer.parseInt(value));
		} else {
			newValue = value;
		}
		
		line.replace(startPos,  endPos,  newValue);
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
		
		this.randomGenerator = new Random(this.tenantId);
	}
	
}
