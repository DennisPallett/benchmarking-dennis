package procedures;

import org.voltdb.*;

public class Set1 extends VoltProcedure {
	public final SQLStmt Query1 = new SQLStmt("SELECT " +
			" f.year_key," +
			" f.month_key," +
			" m.month_name," +
			" f.year_key * 100 + f.month_key AS maand," +
			" m.month_name || ' ' || f.year_key AS maand_naam," +
			" SUM(f.m_budgetbedrag) AS begroting," +
			" SUM(f.m_realisatiebedrag) AS realisatie" +
			" FROM fact_exploitatie AS f, month_names AS m, dim_grootboek AS g, closure_organisatie AS c" +
			" WHERE f.month_key=m.month" +
			" AND f.grootboek_key=g.grootboek_key" +
			" AND f.organisatie_key = c.organisatie_key" +
			" AND c.parent = ?" +
			" AND (g.gb_verdichting_code_1 = 3 OR g.gb_verdichting_code_1 = 4 OR g.gb_verdichting_code_1 = 5)" +
			" AND f.month_key >= 06" +
			" AND f.month_key <= 11" +
			" AND f.year_key = ?" +
			" GROUP BY f.year_key, f.month_key, m.month_name" +
			" ORDER BY f.year_key, f.month_key, m.month_name",
			"closure_organisatie,fact_exploitatie,dim_grootboek,month_names"
	);
	
	public final SQLStmt Query2 = new SQLStmt(
			" SELECT" +
			" g.gb_verdichting_toonnaam_1," +
			" SUM( f.m_budgetbedrag ) AS begroting," +
			" SUM( f.m_realisatiebedrag ) AS realisatie" +
			" FROM fact_exploitatie AS f, dim_grootboek AS g, closure_organisatie AS c" +
			" WHERE f.grootboek_key = g.grootboek_key" +
			" AND f.organisatie_key = c.organisatie_key" +
			" AND c.parent = ?" +
			" AND (g.gb_verdichting_code_1 = 3 OR g.gb_verdichting_code_1 = 4 OR g.gb_verdichting_code_1 = 5)" +
			" AND f.month_key >= 06" +
			" AND f.month_key <= 11" +
			" AND f.year_key = ?" +
			" GROUP BY g.gb_verdichting_toonnaam_1" +
			" ORDER BY g.gb_verdichting_toonnaam_1",
			"closure_organisatie,fact_exploitatie,dim_grootboek"
	);
	
	public final SQLStmt Query3 = new SQLStmt(
			" SELECT" +
			" o.id," +
			" o.naam," +
			" SUM( f.m_budgetbedrag ) AS begroting," +
			" SUM( f.m_realisatiebedrag ) AS realisatie  " +
			" FROM fact_exploitatie AS f, dim_grootboek AS g, closure_organisatie AS c, organisatie AS o" +
			" WHERE f.grootboek_key = g.grootboek_key" +
			" AND f.organisatie_key = c.organisatie_key" +
			" AND c.parent = ?" +
			" AND o.id = c.parent" +
			" AND (g.gb_verdichting_code_1 = 3 OR g.gb_verdichting_code_1 = 4 OR g.gb_verdichting_code_1 = 5)" +
			" AND f.month_key >= 06" +
			" AND f.month_key <= 11" +
			" AND f.year_key = ?" +
			" GROUP BY o.id, o.naam" +
			" ORDER BY o.id, o.naam",
			"closure_organisatie,fact_exploitatie,dim_grootboek,organisatie"
		);
	
	public final SQLStmt Query4 = new SQLStmt(
			" SELECT" +
			" g.gb_verdichting_toonnaam_1," +
			" g.gb_verdichting_toonnaam_2," +
			" g.gb_verdichting_toonnaam_3," +
			" SUM( f.m_budgetbedrag ) AS begroting," +
			" SUM( f.m_realisatiebedrag ) AS realisatie  " +
			" FROM fact_exploitatie AS f, dim_grootboek AS g, closure_organisatie AS c" +
			" WHERE f.grootboek_key = g.grootboek_key" +
			" AND f.organisatie_key = c.organisatie_key" +
			" AND c.parent = ?" +
			" AND g.gb_verdichting_code_1 = 4" +
			" AND g.gb_verdichting_code_2 = 45" +
			" AND f.month_key = 10" +
			" AND f.year_key = ?" +
			" GROUP BY g.gb_verdichting_toonnaam_1, g.gb_verdichting_toonnaam_2, g.gb_verdichting_toonnaam_3" +
			" ORDER BY g.gb_verdichting_toonnaam_1, g.gb_verdichting_toonnaam_2, g.gb_verdichting_toonnaam_3"
			,
			"closure_organisatie,fact_exploitatie,dim_grootboek"
		);
	
	public VoltTable[] run (int yearKey, int parentId) throws VoltAbortException {
		VoltTable[] queryResults;
		
		voltQueueSQL(Query1, parentId, yearKey);
		voltQueueSQL(Query2, parentId, yearKey);
		voltQueueSQL(Query3, parentId, yearKey);
		voltQueueSQL(Query4, parentId, yearKey);
		
		queryResults = voltExecuteSQL(true);
		
		return queryResults;
	}

}
