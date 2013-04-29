package topicus.loadschema;

import java.util.ArrayList;

import topicus.databases.DbColumn;
import topicus.databases.DbTable;

public class DbSchema {
	
	public static ArrayList<DbTable> AllTables () {
		ArrayList<DbTable> ret = new ArrayList<DbTable>();
		
		ret.add(DbSchema.AdminstratieTable());
		ret.add(DbSchema.GrootboekTable());
		ret.add(DbSchema.KostenplaatsTable());
		ret.add(DbSchema.OrganisatieTable());
		ret.add(DbSchema.ClosureTable());
		ret.add(DbSchema.MonthnamesTable());
		ret.add(DbSchema.TijdTable());
		ret.add(DbSchema.FactTable());
		
		return ret;
	}
		
	public static DbTable KostenplaatsTable() {
		DbTable table = new DbTable("dim_kostenplaats");
		
		table.addColumn(
			new DbColumn("kostenplaats_key")
			.setType(DbColumn.Type.INTEGER)
			.setPrimaryKey()
		);
		
		table.addColumn(
			new DbColumn("kp_tenant")
			.setType(DbColumn.Type.INTEGER)
		);
		
		table.addColumn(
			new DbColumn("kp_naam")
			.setType(DbColumn.Type.VARCHAR, 120)
		);
		
		table.addColumn(
			new DbColumn("kp_code")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		table.addColumn(
			new DbColumn("kp_alternatieve_code")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		DbSchema._addStandardCols(table, "kp");	
		
		return table;
	}
	
	public static DbTable OrganisatieTable () {
		DbTable table = new DbTable("organisatie");
		
		table.addColumn(
				new DbColumn("id")
				.setType(DbColumn.Type.INTEGER)
				.setPrimaryKey()
			);
			
			table.addColumn(
				new DbColumn("version")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
			
			table.addColumn(
				new DbColumn("sorteervolgorde")
				.setType(DbColumn.Type.INTEGER)
			);
			
			table.addColumn(
				new DbColumn("actief")
				.setType(DbColumn.Type.SMALL_INTEGER)
			);
			
			table.addColumn(
				new DbColumn("financieeladminnummer")
				.setType(DbColumn.Type.VARCHAR, 10)
				.setAllowNull()
			);
			
			table.addColumn(
				new DbColumn("financieelcode")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
			
			table.addColumn(
				new DbColumn("hrmcode")
				.setType(DbColumn.Type.VARCHAR, 10)
				.setAllowNull()
			);
			
			table.addColumn(
				new DbColumn("naam")
				.setType(DbColumn.Type.VARCHAR, 200)
			);
			
			table.addColumn(
				new DbColumn("organisatieniveau")
				.setType(DbColumn.Type.INTEGER)
			);
			
			table.addColumn(
				new DbColumn("parent")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
			
			table.addColumn(
				new DbColumn("afnemer")
				.setType(DbColumn.Type.INTEGER)
			);
		
		return table;
	}
	
	public static DbTable ClosureTable () {
		DbTable table = new DbTable("closure_organisatie");
		
		table.addColumn(
				new DbColumn("organisatie_key")
				.setType(DbColumn.Type.INTEGER)
			);
			
			table.addColumn(
				new DbColumn("parent")
				.setType(DbColumn.Type.INTEGER)
			);
		
		return table;
	}
	
	public static DbTable MonthnamesTable () {
		DbTable table = new DbTable("month_names");
		
		table.addColumn(
				new DbColumn("month")
				.setType(DbColumn.Type.INTEGER)
				.setPrimaryKey()
			);
			
			table.addColumn(
				new DbColumn("month_name")
				.setType(DbColumn.Type.VARCHAR, 9)
				.setUnique()
			);
		
		return table;		
	}
	
	public static DbTable TijdTable () {
		DbTable table = new DbTable("dim_tijdtabel");
		
		table.addColumn(
				new DbColumn("pk_date")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				.setPrimaryKey()
			);
			
			table.addColumn(
				new DbColumn("financial_month")
				.setType(DbColumn.Type.INTEGER)
			);
			
			table.addColumn(
				new DbColumn("date_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
			
			table.addColumn(
				new DbColumn("year")
				.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				.setAllowNull()
			);
			
			table.addColumn(
				new DbColumn("year_name")
				.setType(DbColumn.Type.VARCHAR, 50)
				.setAllowNull()
			);
			
			table.addColumn(
				new DbColumn("year_number")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull()
			);
			
		
			table.addColumn(
					new DbColumn("quarter")
					.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("quarter_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("month")
					.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("month_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("week")
					.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("week_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("day_of_year")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("day_of_year_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("day_of_quarter")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("day_of_quarter_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("day_of_month")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("day_of_month_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("day_of_week")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("day_of_week_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("week_of_year")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("week_of_year_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);


			table.addColumn(
					new DbColumn("month_of_year")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("month_of_year_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("month_of_quarter")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("month_of_quarter_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("quarter_of_year")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("quarter_of_year_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("quarter_of_half_year")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("quarter_of_half_year_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("month_name_of_year")
					.setType(DbColumn.Type.VARCHAR, 10)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("school_year")
					.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("school_year_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("year_month_index")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("year_month_day_index")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("month_of_year_index_name")
					.setType(DbColumn.Type.VARCHAR, 50)
					.setAllowNull()
				);
		
		return table;		
	}
	
	public static DbTable FactTable () {
		DbTable table = new DbTable("fact_exploitatie");
		table.setPartitionBy("year_key");
		
		table.addColumn(
				new DbColumn("dwh_id")
				.setType(DbColumn.Type.INTEGER)
				.setPrimaryKey()
			);
			
			table.addColumn(
					new DbColumn("tenant_key")
					.setType(DbColumn.Type.INTEGER)
				);
			
			table.addColumn(
					new DbColumn("organisatie_key")
					.setType(DbColumn.Type.INTEGER)
					.setForeignKey("organisatie", "id")
				);
			
			table.addColumn(
					new DbColumn("administratie_key")
					.setType(DbColumn.Type.INTEGER)
					.setForeignKey("dim_administratie", "administratie_key")
				);
			
			table.addColumn(
					new DbColumn("kostenplaats_key")
					.setType(DbColumn.Type.INTEGER)
					.setForeignKey("dim_kostenplaats", "kostenplaats_key")
				);
			
			table.addColumn(
					new DbColumn("kostendrager_key")
					.setType(DbColumn.Type.INTEGER)
				);
			
			table.addColumn(
					new DbColumn("year_key")
					.setType(DbColumn.Type.INTEGER)
				);
			
			table.addColumn(
					new DbColumn("month_key")
					.setType(DbColumn.Type.INTEGER)
					.setForeignKey("month_names", "month")
				);
			
			table.addColumn(
					new DbColumn("project_key")
					.setType(DbColumn.Type.INTEGER)
				);
			
			table.addColumn(
					new DbColumn("grootboek_key")
					.setType(DbColumn.Type.INTEGER)
					.setForeignKey("dim_grootboek", "grootboek_key")
				);
			
			table.addColumn(
					new DbColumn("m_realisatiebedrag")
					.setType(DbColumn.Type.DOUBLE)
				);
			
			table.addColumn(
					new DbColumn("m_budgetbedrag")
					.setType(DbColumn.Type.DOUBLE)
				);
					
			table.addColumn(
					new DbColumn("t_boekstuknummer")
					.setType(DbColumn.Type.VARCHAR, 120)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("t_datum")
					.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				);
			
			table.addColumn(
					new DbColumn("t_omschrijving")
					.setType(DbColumn.Type.VARCHAR, 120)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("t_factuurnummer")
					.setType(DbColumn.Type.VARCHAR, 120)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("dwh_modified_date")
					.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
				);
			
			table.addColumn(
					new DbColumn("t_source_modified")
					.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
					.setAllowNull()
				);
			
			table.addColumn(
					new DbColumn("t_source_id")
					.setType(DbColumn.Type.INTEGER)
					.setAllowNull()
				);
		
		return table;		
	}
	
	public static DbTable GrootboekTable () {
		DbTable table = new DbTable("dim_grootboek");
		
		table.addColumn(
				new DbColumn("grootboek_key")
				.setType(DbColumn.Type.INTEGER)
				.setPrimaryKey()
			);
			
			table.addColumn(
				new DbColumn("gb_tenant")
				.setType(DbColumn.Type.INTEGER)
			);
			
			table.addColumn(
				new DbColumn("gb_naam")
				.setType(DbColumn.Type.VARCHAR, 120)
			);
			
			table.addColumn(
				new DbColumn("gb_rekeningnr")
				.setType(DbColumn.Type.INTEGER)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_code_1")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull(true)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_naam_1")
				.setType(DbColumn.Type.VARCHAR, 120)
				.setAllowNull(true)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_toonnaam_1")
				.setType(DbColumn.Type.VARCHAR, 130)
				.setAllowNull(true)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_code_2")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull(true)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_naam_2")
				.setType(DbColumn.Type.VARCHAR, 120)
				.setAllowNull(true)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_toonnaam_2")
				.setType(DbColumn.Type.VARCHAR, 130)
				.setAllowNull(true)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_code_3")
				.setType(DbColumn.Type.INTEGER)
				.setAllowNull(true)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_naam_3")
				.setType(DbColumn.Type.VARCHAR, 120)
				.setAllowNull(true)
			);
			
			table.addColumn(
				new DbColumn("gb_verdichting_toonnaam_3")
				.setType(DbColumn.Type.VARCHAR, 130)
				.setAllowNull(true)
			);
			
			DbSchema._addStandardCols(table, "gb");	
		
		return table;
	}

	public static DbTable AdminstratieTable () {
		DbTable table = new DbTable("dim_administratie");
		
		table.addColumn(
			new DbColumn("administratie_key")
			.setType(DbColumn.Type.INTEGER)
			.setPrimaryKey()
		);
		
		table.addColumn(
			new DbColumn("a_tenant")
			.setType(DbColumn.Type.INTEGER)
		);
		
		table.addColumn(
			new DbColumn("a_naam")
			.setType(DbColumn.Type.VARCHAR, 120)
		);
		
		table.addColumn(
			new DbColumn("a_code")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);
		
		table.addColumn(
			new DbColumn("a_alternatieve_code")
			.setType(DbColumn.Type.VARCHAR, 10)
			.setAllowNull()
		);		
		
		DbSchema._addStandardCols(table, "a");
		
		return table;
	}
	
	public static void _addStandardCols(DbTable table, String prefix) {
		table.addColumn(
			new DbColumn("dwh_modified_date")
			.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
		);
		
		table.addColumn(
			new DbColumn(prefix + "_source_modified")
			.setType(DbColumn.Type.TIMESTAMP_WITHOUT_TIMEZONE)
			.setAllowNull()
		);
		
		table.addColumn(
			new DbColumn(prefix + "_source_id")
			.setType(DbColumn.Type.INTEGER)
			.setAllowNull()
		);
		
		table.addColumn(
			new DbColumn(prefix + "_source_adminnr")
			.setType(DbColumn.Type.VARCHAR, 20)
		);
	}
	
}
