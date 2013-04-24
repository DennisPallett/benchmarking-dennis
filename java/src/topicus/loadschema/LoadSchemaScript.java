package topicus.loadschema;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.cli.CommandLine;

import au.com.bytecode.opencsv.CSVWriter;

import topicus.ConsoleScript;
import topicus.DatabaseScript;
import topicus.ExitCodes;
import topicus.benchmarking.BenchmarksScript.MissingResultsFileException;
import topicus.databases.AbstractDatabase;
import topicus.databases.DbColumn;
import topicus.loadtenant.LoadTenantScript.AlreadyDeployedException;

public class LoadSchemaScript extends DatabaseScript {
	protected String baseDirectory = "";	
	
	protected Connection conn;

	public LoadSchemaScript(String type, AbstractDatabase database) {
		super(type, database);
	}
	
	public void run () throws Exception {	
		printLine("Started-up schema loading tool");	
		
		this.setupLogging(cliArgs.getOptionValue("log-file"));
		
		printLine("Type set to: " + this.type);
		
		this.printLine("Setting up connection");
		this.conn = this._setupConnection();
		this.printLine("Connection setup");
		
		this._setOptions();
		
		this._checkIfDeployed();
			
		this.printLine("Starting deployment of schema");
		
		this._deploySchema();
		this._loadBaseData();

		this.printLine("Finished deployment");		
		
		this.conn.close();
		this.printLine("Stopping");
	}
	
	protected void _deploySchema() throws SQLException {
		this._deployAdministratieTable();
		this._deployGrootboekTable();
		this._deployKostenplaatsTable();
		this._deployOrganisatieTable();
		this._deployClosureTable();
		this._deployMonthnamesTable();
		this._deployTijdTable();
		this._deployFactTable();
	}
	
	protected void _loadBaseData () throws SQLException {
		printLine("Loading base data");
		
		this.printLine("Loading month names...");
		database.deployData(conn, this.baseDirectory + "month_names.tbl", "month_names");
		this.printLine("Done");		
		
		this.printLine("Loading tijdtabel data...");
		database.deployData(conn, this.baseDirectory + "tijd_data.tbl", "dim_tijdtabel");
		this.printLine("Done");		
		
		printLine("Finished loading base data");
	}
	
	protected void _deployFactTable() throws SQLException {
		this.printLine("Deploying `fact_exploitatie`");
		String tableName = "fact_exploitatie";
		
		database.dropTable(conn, tableName);
		
		ArrayList<DbColumn> cols = new ArrayList<DbColumn>();
		
		cols.add(
			new DbColumn("dwh_id")
			.setType(DbColumn.Type.INTEGER)
			.setPrimaryKey()
		);
		
		cols.add(
				new DbColumn("tenant_key")
				.setType(DbColumn.Type.INTEGER)
			);
		
		cols.add(
				new DbColumn("organisatie_key")
				.setType(DbColumn.Type.INTEGER)
				.setForeignKey("organisatie", "id")
			);
		
		cols.add(
				new DbColumn("administratie_key")
				.setType(DbColumn.Type.INTEGER)
				.setForeignKey("dim_administratie", "administratie_key")
			);
		
		cols.add(
				new DbColumn("kostenplaats_key")
				.setType(DbColumn.Type.INTEGER)
				.setForeignKey("dim_kostenplaats", "kostenplaats_key")
			);
		
		cols.add(
				new DbColumn("kostendrager_key")
				.setType(DbColumn.Type.INTEGER)
			);
		
		cols.add(
				new DbColumn("year_key")
				.setType(DbColumn.Type.INTEGER)
			);
		
		cols.add(
				new DbColumn("month_key")
				.setType(DbColumn.Type.INTEGER)
				.setForeignKey("month_names", "month")
			);
		
		cols.add(
				new DbColumn("project_key")
				.setType(DbColumn.Type.INTEGER)
			);
		
		cols.add(
				new DbColumn("grootboek_key")
				.setType(DbColumn.Type.INTEGER)
				.setForeignKey("dim_grootboek", "grootboek_key")
			);
		
		cols.add(
				new DbColumn("m_realisatiebedrag")
				.setType(DbColumn.Type.DOUBLE)
			);
		
		cols.add(
				new DbColumn("m_budgetbedrag")
				.setType(DbColumn.Type.DOUBLE)
			);
				
		cols.add(
				new DbColumn("t_boekstuknummer")
				.setType(DbColumn.Type.VARCHAR, 120)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("t_datum")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
			);
		
		cols.add(
				new DbColumn("t_omschrijving")
				.setType(DbColumn.Type.VARCHAR, 120)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("t_factuurnummer")
				.setType(DbColumn.Type.VARCHAR, 120)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("dwh_modified_date")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
			);
		
		cols.add(
				new DbColumn("t_source_modified")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("t_source_id")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		this.database.createTable(this.conn, tableName, cols, "year_key");		
		this.printLine("Table deployed");		
	}
	
	protected void _deployTijdTable() throws SQLException {
		this.printLine("Deploying `dim_tijdtabel`");
		String tableName = "dim_tijdtabel";
		
		database.dropTable(conn, tableName);
		
		ArrayList<DbColumn> cols = new ArrayList<DbColumn>();
		
		cols.add(
			new DbColumn("pk_date")
			.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
			.setPrimaryKey()
		);
		
		cols.add(
			new DbColumn("financial_month")
			.setType(DbColumn.Type.INTEGER)
		);
		
		cols.add(
			new DbColumn("date_name")
			.setType(DbColumn.Type.VARCHAR, 50)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("year")
			.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("year_name")
			.setType(DbColumn.Type.VARCHAR, 50)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("year_number")
			.setType(DbColumn.Type.INTEGER)
			.setAllowNull()
		);
		
	
		cols.add(
				new DbColumn("quarter")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("quarter_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("month")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("month_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("week")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("week_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("day_of_year")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("day_of_year_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("day_of_quarter")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("day_of_quarter_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("day_of_month")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("day_of_month_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("day_of_week")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("day_of_week_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("week_of_year")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("week_of_year_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);


		cols.add(
				new DbColumn("month_of_year")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("month_of_year_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("month_of_quarter")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("month_of_quarter_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("quarter_of_year")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("quarter_of_year_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("quarter_of_half_year")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("quarter_of_half_year_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("month_name_of_year")
				.setType(DbColumn.Type.VARCHAR, 10)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("school_year")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("school_year_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("year_month_index")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("year_month_day_index")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
		
		cols.add(
				new DbColumn("month_of_year_index_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);

		
		this.database.createTable(this.conn, tableName, cols);		
		this.printLine("Table deployed");		
	}
	
	protected void _deployMonthnamesTable() throws SQLException {
		this.printLine("Deploying `month_names`");
		String tableName = "month_names";
		
		database.dropTable(conn, tableName);
		
		ArrayList<DbColumn> cols = new ArrayList<DbColumn>();
		
		cols.add(
			new DbColumn("month")
			.setType(DbColumn.Type.INTEGER)
			.setPrimaryKey()
		);
		
		cols.add(
			new DbColumn("month_name")
			.setType(DbColumn.Type.VARCHAR, 9)
			.setUnique()
		);
		
		this.database.createTable(this.conn, tableName, cols);		
		this.printLine("Table deployed");		
	}
	
	protected void _deployClosureTable() throws SQLException {
		this.printLine("Deploying `closure_organisatie`");
		String tableName = "closure_organisatie";
		
		database.dropTable(conn, tableName);
		
		ArrayList<DbColumn> cols = new ArrayList<DbColumn>();
		
		cols.add(
			new DbColumn("organisatie_key")
			.setType(DbColumn.Type.INTEGER)
		);
		
		cols.add(
			new DbColumn("parent")
			.setType(DbColumn.Type.INTEGER)
		);
		
		this.database.createTable(this.conn, tableName, cols);		
		this.printLine("Table deployed");			
	}
	
	protected void _deployOrganisatieTable() throws SQLException {
		this.printLine("Deploying `organisatie`");
		String tableName = "organisatie";
		
		database.dropTable(conn, tableName);
		
		ArrayList<DbColumn> cols = new ArrayList<DbColumn>();
		
		cols.add(
			new DbColumn("id")
			.setType(DbColumn.Type.INTEGER)
			.setPrimaryKey()
		);
		
		cols.add(
			new DbColumn("version")
			.setType(DbColumn.Type.INTEGER)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("sorteervolgorde")
			.setType(DbColumn.Type.INTEGER)
		);
		
		cols.add(
			new DbColumn("actief")
			.setType(DbColumn.Type.SMALL_INTEGER)
		);
		
		cols.add(
			new DbColumn("financieeladminnummer")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("financieelcode")
			.setType(DbColumn.Type.VARCHAR, 50)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("hrmcode")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("naam")
			.setType(DbColumn.Type.VARCHAR, 200)
		);
		
		cols.add(
			new DbColumn("organisatieniveau")
			.setType(DbColumn.Type.INTEGER)
		);
		
		cols.add(
			new DbColumn("parent")
			.setType(DbColumn.Type.INTEGER)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("afnemer")
			.setType(DbColumn.Type.INTEGER)
		);
		
		this.database.createTable(this.conn, tableName, cols);		
		this.printLine("Table deployed");			
	}
	
	protected void _deployKostenplaatsTable() throws SQLException {
		this.printLine("Deploying `dim_kostenplaats");
		String tableName = "dim_kostenplaats";
		
		database.dropTable(conn, tableName);
		
		ArrayList<DbColumn> cols = new ArrayList<DbColumn>();
		
		cols.add(
			new DbColumn("kostenplaats_key")
			.setType(DbColumn.Type.INTEGER)
			.setPrimaryKey()
		);
		
		cols.add(
			new DbColumn("kp_tenant")
			.setType(DbColumn.Type.INTEGER)
		);
		
		cols.add(
			new DbColumn("kp_naam")
			.setType(DbColumn.Type.VARCHAR, 120)
		);
		
		cols.add(
			new DbColumn("kp_code")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("kp_alternatieve_code")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		this._addStandardCols(cols, "kp");		
		
		this.database.createTable(this.conn, tableName, cols);		
		this.printLine("Table deployed");	
	}
	
	protected void _deployGrootboekTable () throws SQLException {
		this.printLine("Deploying `dim_grootboek`");
		
		String tableName = "dim_grootboek";
		
		database.dropTable(conn, tableName);
		
		ArrayList<DbColumn> cols = new ArrayList<DbColumn>();
		
		cols.add(
			new DbColumn("grootboek_key")
			.setType(DbColumn.Type.INTEGER)
			.setPrimaryKey()
		);
		
		cols.add(
			new DbColumn("gb_tenant")
			.setType(DbColumn.Type.INTEGER)
		);
		
		cols.add(
			new DbColumn("gb_naam")
			.setType(DbColumn.Type.VARCHAR, 120)
		);
		
		cols.add(
			new DbColumn("gb_rekeningnr")
			.setType(DbColumn.Type.INTEGER)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_code_1")
			.setType(DbColumn.Type.INTEGER)
			.setAllowNull(true)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_naam_1")
			.setType(DbColumn.Type.VARCHAR, 120)
			.setAllowNull(true)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_toonnaam_1")
			.setType(DbColumn.Type.VARCHAR, 130)
			.setAllowNull(true)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_code_2")
			.setType(DbColumn.Type.INTEGER)
			.setAllowNull(true)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_naam_2")
			.setType(DbColumn.Type.VARCHAR, 120)
			.setAllowNull(true)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_toonnaam_2")
			.setType(DbColumn.Type.VARCHAR, 130)
			.setAllowNull(true)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_code_3")
			.setType(DbColumn.Type.INTEGER)
			.setAllowNull(true)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_naam_3")
			.setType(DbColumn.Type.VARCHAR, 120)
			.setAllowNull(true)
		);
		
		cols.add(
			new DbColumn("gb_verdichting_toonnaam_3")
			.setType(DbColumn.Type.VARCHAR, 130)
			.setAllowNull(true)
		);
		
		this._addStandardCols(cols, "gb");		
		
		this.database.createTable(this.conn, tableName, cols);		
		this.printLine("Table deployed");		
	}
	
	protected void _deployAdministratieTable () throws SQLException {
		this.printLine("Deploying `dim_administratie`");
		
		this.database.dropTable(this.conn,  "dim_administratie");
		
		ArrayList<DbColumn> cols = new ArrayList<DbColumn>();
		
		cols.add(
			new DbColumn("administratie_key")
			.setType(DbColumn.Type.INTEGER)
			.setPrimaryKey()
		);
		
		cols.add(
			new DbColumn("a_tenant")
			.setType(DbColumn.Type.INTEGER)
		);
		
		cols.add(
			new DbColumn("a_naam")
			.setType(DbColumn.Type.VARCHAR, 120)
		);
		
		cols.add(
			new DbColumn("a_code")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn("a_alternatieve_code")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		this._addStandardCols(cols, "a");
		
		this.database.createTable(this.conn, "dim_administratie", cols);		
		
		this.printLine("Table deployed");
	}
	
	protected void _addStandardCols(ArrayList<DbColumn> cols, String prefix) {
		cols.add(
			new DbColumn("dwh_modified_date")
			.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
		);
		
		cols.add(
			new DbColumn(prefix + "_source_modified")
			.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn(prefix + "_source_id")
			.setType(DbColumn.Type.INTEGER)
			.setAllowNull()
		);
		
		cols.add(
			new DbColumn(prefix + "_source_adminnr")
			.setType(DbColumn.Type.VARCHAR, 20)
		);
	}
	
	protected void _checkIfDeployed () throws Exception {
		if (this.isSchemaDeployed()) {
			this.printError("Schema is already deployed!");
			
			if (this.cliArgs.hasOption("stop-on-deployed")) {
				throw new AlreadyDeployedException("Schema is already deployed");
			} else {
				boolean doDeploy = this.confirmBoolean("Are you sure you want to re-deploy schema? This will delete any old data! (y/n)");
				if (!doDeploy) {
					printLine("Stopping");
					throw new CancelledException("Cancelled by user");
				}				
			}
		}
	}
	
	protected boolean isSchemaDeployed () throws SQLException {
		// do a simple count on month names to determine table exists
		Statement q = this.conn.createStatement();
		
		ResultSet result = null;
		try {
			result = q.executeQuery("SELECT * FROM month_names LIMIT 1");
		} catch (SQLException e) {
			// fail, likely because no table there
		}
		
		boolean ret = (result != null && result.next());
		
		if (result != null) result.close();
		
		return ret;
	}
	
	protected void _setOptions () throws Exception {	
		this.baseDirectory = this.cliArgs.getOptionValue("base-data", "/benchmarking/data/base-data/");
		if (this.baseDirectory.length() == 0) {
			throw new MissingBaseDataDirectoryException("Missing base data directory!");
		}
		
		if (this.baseDirectory.endsWith("/") == false
			&& this.baseDirectory.endsWith("\\") == false) {
				this.baseDirectory += "/";
		}
	}
	
	public class CancelledException extends Exception {
		public CancelledException(String string) {
			super(string);
		}
	}
	
	public class MissingBaseDataDirectoryException extends Exception {
		public MissingBaseDataDirectoryException(String string) {
			super(string);
		}
	}
	
	public class AlreadyDeployedException extends Exception {
		public AlreadyDeployedException(String string) {
			super(string);
		}
	}

	
}
