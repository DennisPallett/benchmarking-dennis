package topicus.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
	
	public class MissingDataFileException extends Exception {
		public MissingDataFileException(String string) {
			super(string);
		}
	}
	
	public class FailedToWriteException extends Exception {
		public FailedToWriteException(String string) {
			super(string);
		}
	}
	
	protected String outputDirectory;
	protected int tenantId;
	protected int[] tenantIds;
	protected String dataDirectory;
	protected String tenantDirectory;
	
	protected boolean skipExisting = false;
	
	protected Random randomGenerator;
	
	protected HashMap<String, Integer> rowCounts = new HashMap<String, Integer>();
	
	public void run () throws CancelledException, InvalidDataDirectoryException, 
	InvalidTenantIdException, InvalidOutputDirectoryException, MissingDataFileException, FailedToWriteException {
		printLine("Started-up tenant generator tool");	
				
		this._setOptions();
		
		if (!this.cliArgs.hasOption("start")) {
			if (!confirmBoolean("Start generating data for tenant " + Arrays.toString(tenantIds) + "? (y/n)")) {
				printError("Stopping");
				throw new CancelledException();
			}
		}
		
		for(int tenantId : tenantIds) {
			this._generateTenant(tenantId);
		}	
		
		this.printLine("Successfully finished!");
	}
	
	protected void _generateTenant(int tenantId) throws CancelledException, MissingDataFileException, FailedToWriteException {
		printLine("Generating tenant #" + tenantId);
		
		this.tenantId = tenantId;
		this.tenantDirectory = this.outputDirectory + tenantId + "/";
		
		if (this._checkTenantExists() == false) {
			return;
		}
		
		this._createTenantFile("adm_data.tbl");
		this._createTenantFile("org_data.tbl");
		this._createTenantFile("closure_org_data.tbl");
		this._createTenantFile("gb_data.tbl");
		this._createTenantFile("kp_data.tbl");
		
		this._createTenantFactFile();	
		
		printLine("Generated tenant #" + tenantId);
	}
	
	protected void _createTenantFactFile () throws FailedToWriteException, MissingDataFileException {
		printLine("Generating fact file");
		
		File file = new File(this.dataDirectory + "fe_data.tbl");
		
		// load data in memory
		this.printLine("Loading fact file into memory");
		String[] lines = new String[3740431];
		Scanner scan;
		try {
			scan = new Scanner(file).useDelimiter("\n");
		} catch (FileNotFoundException e1) {
			throw new MissingDataFileException("Missing data file `fe_data.tbl` in data directory!");
		}
		int i = 0;
		String tmp = "";
		while(scan.hasNext()) {
			tmp = scan.next();
			
			if (tmp != null && tmp.length() > 0) {
				lines[i] = tmp;
			}
			
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
				if (line == null || line.length() == 0) continue;
				
				StringBuilder newLine = new StringBuilder();
				
				newLine.append(primaryKey);
				newLine.append("#");
				newLine.append(tenantId);
				newLine.append("#");
				newLine.append(line.trim());					
				
				// replace FK's
				this._replaceFK(newLine, "PK_ORG:",  rowCounts.get("org_data.tbl"));
				this._replaceFK(newLine, "PK_ADMIN:",  rowCounts.get("adm_data.tbl"));
				this._replaceFK(newLine, "PK_KP:",  rowCounts.get("kp_data.tbl"));
				this._replaceFK(newLine, "PK_GB:",  rowCounts.get("gb_data.tbl"));
				
				// replace year
				int startPos = newLine.indexOf("YEAR:");
				int endPos = newLine.indexOf("#", startPos);
					
				// retrieve year
				int year = Integer.parseInt(newLine.substring(startPos+("YEAR:").length(), endPos));
				
				// calculate new year
				int newYear = year + (5 * j);
				
				// insert new year into line
				newLine.replace(startPos, endPos, String.valueOf(newYear));
				
				// calculate new tenant_year_key
				long tenantYearKey = ((tenantId * 10000) + newYear);
				
				// replace value
				startPos = newLine.indexOf("VALUE:");
				if (startPos > -1) {
					endPos = newLine.indexOf("#", startPos);
					
					// retrieve value
					double value = Float.parseFloat(newLine.substring(startPos+("VALUE:").length(), endPos));
					
					// multiply by random multiplier
					// original value is used for the first part of #tenant 1
					// so that the original Carmel data is also within the test data
					if (tenantId != 1 || j != 0) {
						value = value * randFloat;
					}
					
					// insert new value into line
					newLine.replace(startPos, endPos,  String.valueOf(value));
				}
				
				newLine.append("#");
				newLine.append(tenantYearKey);
				newLine.append("\n");
				
				stringBuffer.append(newLine);
				
				if (lineCounter % 100000 == 0) {
					printLine("Processed " + lineCounter + " rows");
					randFloat = randomGenerator.nextInt(500) / 100;
				}
				
				if (lineCounter % 1000000 == 0) {
					printLine("Writing lines");
					try {
						FileUtils.writeStringToFile(newFile, stringBuffer.toString(), true);
					} catch (IOException e) {
						throw new FailedToWriteException("Unable to write fact file");
					}
					stringBuffer = new StringBuilder((int)file.length()/3);
				}
				
				lineCounter++;
				primaryKey++;
			}
			
			
		}
		
		printLine("Writing lines");
		try {
			FileUtils.writeStringToFile(newFile, stringBuffer.toString(), true);
		} catch (IOException e) {
			throw new FailedToWriteException("Unable to write fact file");
		}
		printLine("Processed " + lineCounter + " rows");
		
		lines = null;
		stringBuffer = null;
		printLine("Finished fact file!");
	}
	
	protected void _createTenantFile(String fileName) throws MissingDataFileException, FailedToWriteException {
		printLine("Creating " + fileName);
		
		File file = new File(this.dataDirectory + fileName);

		// load data in memory
		ArrayList<String> lines = new ArrayList<String>();
		Scanner scan;
		try {
			scan = new Scanner(file).useDelimiter("\n");
		} catch (FileNotFoundException e) {
			throw new MissingDataFileException("Missing data file `" + fileName + "` in data directory!");
		}
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
		try {
			FileUtils.writeStringToFile(new File(this.tenantDirectory + fileName), newContent);
		} catch (IOException e) {
			throw new FailedToWriteException("Unable to write `" + fileName + "` file");
		}
		
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
	
	protected boolean _checkTenantExists() {
		File tenantDir = new File(this.tenantDirectory);
		if (tenantDir.exists()) {
			printError("Tenant directory already exists");
			
			// skip?
			if (skipExisting || confirmBoolean("Do you want to skip this tenant? (y/n)")) {
				return false;
			}
			
			// delete?
			if (!confirmBoolean("Do you want to delete existing tenant directory? (y/n)")) {
				return false;
			}
			
			// yes, delete existing tenant directory
			try {
				FileUtils.deleteDirectory(tenantDir);
			} catch (IOException e) {
				// don't care?
			}
		}
		
		// create tenant dir
		tenantDir.mkdir();
		this.printLine("Created tenant directory");
		return true;
	}
	
	protected void _setOptions () throws InvalidDataDirectoryException, InvalidTenantIdException, InvalidOutputDirectoryException {
		// output directory
		this.outputDirectory = cliArgs.getOptionValue("output", "");
		File outputDir = new File(this.outputDirectory, "");
		if (this.outputDirectory.length() == 0 || outputDir.exists() == false || outputDir.isFile()) {
			throw new InvalidOutputDirectoryException();
		}
		this.outputDirectory = outputDir.getAbsolutePath() + "/";
		this.printLine("Output directory: " + this.outputDirectory);
		
		// tenant directory
		this.dataDirectory = cliArgs.getOptionValue("tenant-data-directory", "");
		File dataDir = new File(this.dataDirectory, "");
		if (this.dataDirectory.length() == 0 || dataDir.exists() == false || dataDir.isFile()) {
			throw new InvalidDataDirectoryException();
		}
		this.dataDirectory = dataDir.getAbsolutePath() + "/";
		this.printLine("Using data directory: " + this.dataDirectory);
		
		String tenantId = cliArgs.getOptionValue("id", "");
		if (tenantId.length() == 0) {
			throw new InvalidTenantIdException();
		}
		
		if(tenantId.indexOf("-") < 0) {
			// single ID
			try {
				tenantIds = new int[]{Integer.parseInt(tenantId)};
			} catch (NumberFormatException e) {
				throw new InvalidTenantIdException();
			}
		} else {
			String[] split = tenantId.split("-");
			
			int startId = 0;
			int endId = 0;
			try {
				startId = Integer.parseInt(split[0]);
				endId = Integer.parseInt(split[1]);
			} catch (NumberFormatException e) {
				throw new InvalidTenantIdException();
			}
			
			if (endId - startId < 1) {
				throw new InvalidTenantIdException();
			}
			
			tenantIds = new int[(endId-startId)+1];
			int id = startId;
			for(int i=0; i < tenantIds.length; i++) {
				tenantIds[i] = id;
				id++;
			}			
		}
		
		this.skipExisting = this.cliArgs.hasOption("skip-existing");
		if (skipExisting) {
			printLine("Automatically skipping existing tenants");
		}
		
		this.printLine("Tenant ID set to: " + Arrays.toString(this.tenantIds));
			
		this.randomGenerator = new Random(this.tenantId);
	}
	
}
