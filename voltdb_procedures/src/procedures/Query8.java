package procedures;

import org.voltdb.*;

public class Query8 extends AbstractQuery {
	public final SQLStmt GetResult = new SQLStmt("SELECT " +
			" f.year_key," +
			" f.month_key," +
			" m.month_name," +
			" f.year_key * 100 + f.month_key AS maand," +
			" m.month_name || ' ' || f.year_key AS maand_naam," +
			" '(3) Baten'," +
			" SUM(f.m_budgetbedrag) AS begroting," +
			" SUM(f.m_realisatiebedrag) AS realisatie" +
			" FROM fact_exploitatie AS f, month_names AS m, dim_grootboek AS g" +
			" WHERE f.month_key=m.month" +
			" AND f.grootboek_key=g.grootboek_key" +
			" AND " + OrganisationClause +
			" AND g.gb_verdichting_toonnaam_1 = '(3) Baten'" +
			" AND f.month_key >= 06" +
			" AND f.month_key <= 11" +
			" AND f.year_key = ?" +
			" GROUP BY f.year_key, f.month_key, m.month_name" +
			" ORDER BY f.year_key, f.month_key, m.month_name");
	
	public VoltTable run (int yearKey, int parentId) throws VoltAbortException {
		VoltTable[] queryResults;
		VoltTable result;
		
		long[] orgIds = this.getOrgIds(parentId);
		long min = orgIds[0];
		long max = orgIds[1];
		
		voltQueueSQL(GetResult, parentId, min, max, yearKey); 
		queryResults = voltExecuteSQL();
		
		result = queryResults[0];		
		return result;
	}

}
