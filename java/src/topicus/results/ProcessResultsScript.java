package topicus.results;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import au.com.bytecode.opencsv.CSVReader;

import topicus.ConsoleScript;

public class ProcessResultsScript extends ConsoleScript {
	class InvalidResultsDirectoryException extends Exception {}
	class InvalidDataDirectoryException extends Exception {}
	class InvalidTenantIdException extends Exception {}
	class CancelledException extends Exception {}
	
	protected String resultsDirectory;
	
	protected Connection conn;
	
	public void run () throws Exception {
		printLine("Started-up resutls processing tool");	
				
		this._setOptions();
		
		if (!this.cliArgs.hasOption("start")) {
			if (!confirmBoolean("Start processing results? (y/n)")) {
				printError("Stopping");
				conn.close();
				throw new CancelledException();
			}
		}
		
		File resDir = new File(this.resultsDirectory);
		this.printLine("Found " + resDir.listFiles().length + " files in directory");
		
		Pattern pBenchmark = Pattern.compile("^benchmark-(.*)-(\\d+)-nodes-(\\d+)-tenants-(\\d+)-users-(\\d+)-iterations$");
		Pattern pLoad = Pattern.compile("^load-(.*)-(\\d+)-nodes-tenant-(\\d+)$");
		
		for(File file : resDir.listFiles()) {
			String fileName = file.getName();
			
			Matcher mBenchmark = pBenchmark.matcher(fileName);			
			Matcher mLoad = pLoad.matcher(fileName);
			if (mBenchmark.find()) {			
				this._parseBenchmarkFile(
					file, 
					mBenchmark.group(1),
					Integer.parseInt(mBenchmark.group(2)),
					Integer.parseInt(mBenchmark.group(3)), 
					Integer.parseInt(mBenchmark.group(4)),
					Integer.parseInt(mBenchmark.group(5))
				);
			} else if (mLoad.find()) {
				this._parseLoadFile(
					file,
					mLoad.group(1),
					Integer.parseInt(mLoad.group(2)),
					Integer.parseInt(mLoad.group(3))
				);				
			} else {
				this.printLine("Found unknown file `" + fileName + "`, skipping!");
			}

		}
			
		this.printLine("Successfully finished!");
		conn.close();
	}
	
	protected void _parseLoadFile (File file, String type, int nodes, int tenantId) throws Exception {
		this.printLine("Found load results file for `" + type + "` for " + nodes + " nodes and tenant #" + tenantId);
		
		int productId = this.getProductId(type);
		if (productId == -1) {
			throw new InvalidTypeException("Invalid type `"+ type + "`");
		}
		
		PreparedStatement q;
		ResultSet result;
		
		// check if this file already has results stored
		q = conn.prepareStatement("SELECT * FROM `load` " +
				"WHERE load_product = ? AND load_nodes = ? AND load_tenant = ?");
		q.setInt(1,  productId);
		q.setInt(2, nodes);
		q.setInt(3, tenantId);
		
		q.execute();
		result = q.getResultSet();
		boolean exists = result.next();
		
		int existingId = -1;
		if (exists) {
			existingId = result.getInt("load_id");
		}
		
		result.close();
		q.close();
		
		if (exists) {
			// already exists, ask to delete old results
			this.printError("There are already results stored for this load!");
			if (confirmBoolean("Delete old results from database? (y/n)")) {
				q = conn.prepareStatement("DELETE FROM `load` WHERE load_id = ?");
				q.setInt(1, existingId);
				q.execute();
				this.printLine("Old results deleted!");
			} else {
				// don't overwrite
				this.printError("Skipping file");
				return;
			}
		}	
		
		// insert benchmark
		q = conn.prepareStatement("INSERT INTO `load` (load_product, load_nodes, load_tenant) " +
				" VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
		q.setInt(1, productId);
		q.setInt(2, nodes);
		q.setInt(3, tenantId);
		q.executeUpdate();
		
		ResultSet keys = q.getGeneratedKeys();
		keys.next();
		
		int loadId = keys.getInt(1);
		
		this.printLine("Parsing file...");
		
		CSVReader reader = new CSVReader(new FileReader(file), '\t');
		List<String[]> rows = reader.readAll();
		
		q = conn.prepareStatement("INSERT INTO load_results (`load`, `table`, rowCount, exectime) VALUES (?, ?, ?, ?)");
		
		int resultCount = 0;
		for (String[] row : rows) {
			String table = row[0];
			int rowCount = Integer.parseInt(row[1]);
			int execTime = Integer.parseInt(row[2]);
						
			q.setInt(1,  loadId);
			q.setString(2,  table);
			q.setInt(3,  rowCount);
			q.setInt(4,  execTime);
						
			q.execute();	
			resultCount++;
		}		
		
		this.printLine("Inserted " + resultCount + " results");
	}
	
	protected void _parseBenchmarkFile (File file, String type, int nodes, int tenants, int users, int iterations) throws Exception {
		this.printLine("Found benchmark results file for `" + type + "` with " + nodes + " nodes, " + tenants + " tenants and " + users + " users");
		
		int productId = this.getProductId(type);
		if (productId == -1) {
			throw new InvalidTypeException("Invalid type `"+ type + "`");
		}
		
		PreparedStatement q;
		ResultSet result;
		
		// check if this file already has results stored
		q = conn.prepareStatement("SELECT * FROM benchmark " +
				"WHERE benchmark_product = ? AND benchmark_nodes = ? AND benchmark_users = ? AND benchmark_tenants = ?");
		q.setInt(1,  productId);
		q.setInt(2, nodes);
		q.setInt(3, users);
		q.setInt(4, tenants);
		
		q.execute();
		result = q.getResultSet();
		boolean exists = result.next();
		
		int existingId = -1;
		if (exists) {
			existingId = result.getInt("benchmark_id");
		}
		
		result.close();
		q.close();
		
		if (exists) {
			// already exists, ask to delete old results
			this.printError("There are already results stored for this benchmark!");
			if (confirmBoolean("Delete old results from database? (y/n)")) {
				q = conn.prepareStatement("DELETE FROM benchmark WHERE benchmark_id = ?");
				q.setInt(1, existingId);
				q.execute();
				this.printLine("Old results deleted!");
			} else {
				// don't overwrite
				this.printError("Skipping file");
				return;
			}
		}		 
		
		// insert benchmark
		q = conn.prepareStatement("INSERT INTO benchmark (benchmark_product, benchmark_nodes, benchmark_users, " +
				"benchmark_tenants, benchmark_iterations) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
		q.setInt(1, productId);
		q.setInt(2, nodes);
		q.setInt(3, users);
		q.setInt(4, tenants);
		q.setInt(5, iterations);
		q.executeUpdate();
		
		ResultSet keys = q.getGeneratedKeys();
		keys.next();
		
		int benchmarkId = keys.getInt(1);
		
		this.printLine("Parsing file...");
		
		CSVReader reader = new CSVReader(new FileReader(file), '\t');
		List<String[]> rows = reader.readAll();
		
		q = conn.prepareStatement("INSERT INTO benchmark_results (benchmark, user, iteration, `set`, query1_time, query2_time, " +
				"query3_time, query4_time, set_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
		
		int rowCount = 0;
		for (String[] row : rows) {
			int userId = Integer.parseInt(row[0]);
			int iteration = Integer.parseInt(row[1]);
			int setId = Integer.parseInt(row[2]);
			int query1_time = Integer.parseInt(row[3]);
			int query2_time = Integer.parseInt(row[4]);
			int query3_time = Integer.parseInt(row[5]);
			int query4_time = Integer.parseInt(row[6]);
			int set_time = Integer.parseInt(row[7]);
			
			q.setInt(1,  benchmarkId);
			q.setInt(2,  userId);
			q.setInt(3,  iteration);
			q.setInt(4,  setId);
			q.setInt(5,  query1_time);
			q.setInt(6,  query2_time);
			q.setInt(7,  query3_time);
			q.setInt(8,  query4_time);
			q.setInt(9,  set_time);
			
			q.execute();	
			rowCount++;
		}		
		
		this.printLine("Inserted " + rowCount + " results");
	}
		
	protected void _setOptions () throws Exception {
		// results directory
		this.resultsDirectory = cliArgs.getOptionValue("results-directory", "");
		File resultsDir = new File(this.resultsDirectory, "");
		if (resultsDir.exists() == false || resultsDir.isFile()) {
			throw new InvalidResultsDirectoryException();
		}
		this.resultsDirectory = resultsDir.getAbsolutePath() + "/";
		this.printLine("Results directory: " + this.resultsDirectory);
		
		String dbUser = cliArgs.getOptionValue("user", "");
		String dbPassword = cliArgs.getOptionValue("password", "");
		String dbName = cliArgs.getOptionValue("database", "benchmarking_results");
		String dbHost = cliArgs.getOptionValue("host", "localhost");
		
		if (cliArgs.hasOption("password") == false) {
			// ask for password
			dbPassword = confirm("Database password: ");			
		}
		
		// try to setup DB connection
		Class.forName("com.mysql.jdbc.Driver");
		String connUrl = "jdbc:mysql://localhost:3306/" + dbName;
		printLine("Setting up connection: " + connUrl);
		conn = DriverManager.getConnection(connUrl, dbUser, dbPassword);
		printLine("Connection setup!");
	}
	
	protected int getProductId(String type) throws SQLException {
		// find product id
		PreparedStatement q = conn.prepareStatement("SELECT product_id FROM product WHERE product_type = ?");
		q.setString(1,  type);
		
		q.execute();
		ResultSet result = q.getResultSet(); 
		
		int productId = -1;
		if (result.next()) {
			productId = result.getInt("product_id");
		}
			
		result.close();
		q.close();
		
		return productId;
	}
	
	public class InvalidTypeException extends Exception {
		public InvalidTypeException(String string) {
			super(string);
		}
	}
}
