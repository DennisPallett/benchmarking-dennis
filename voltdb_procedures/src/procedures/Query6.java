package procedures;

import org.voltdb.*;

public class Query6 extends AbstractQuery {
	public final SQLStmt GetResult = new SQLStmt(
		" SELECT" +
		" g.gb_verdichting_toonnaam_1," + 
		" g.gb_verdichting_toonnaam_2," + 
		" 'All'," +
		" SUM( f.m_budgetbedrag ) AS begroting," +
		" SUM( f.m_realisatiebedrag ) AS realisatie" + 
		" FROM fact_exploitatie AS f, dim_grootboek AS g" +
		" WHERE f.grootboek_key = g.grootboek_key" +
		" AND " + OrganisationClause +
		" AND g.gb_verdichting_toonnaam_1 =  '(3) Baten'" +
		" AND f.month_key >= 06" +
		" AND f.month_key <= 11" +
		" AND f.year_key = ?" +
		" GROUP BY g.gb_verdichting_toonnaam_1, g.gb_verdichting_toonnaam_2" + 
		" ORDER BY g.gb_verdichting_toonnaam_2 ASC"
	);
	
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
