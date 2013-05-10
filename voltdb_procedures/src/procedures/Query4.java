package procedures;

import org.voltdb.*;

public class Query4 extends AbstractQuery {
	public final SQLStmt GetNumbers = new SQLStmt(
		" SELECT" +
		" g.gb_verdichting_toonnaam_1," +
		" g.gb_verdichting_toonnaam_2," +
		" g.gb_verdichting_toonnaam_3," +
		" SUM( f.m_budgetbedrag ) AS begroting," +
		" SUM( f.m_realisatiebedrag ) AS realisatie  " +
		" FROM fact_exploitatie AS f, dim_grootboek AS g" +
		" WHERE f.grootboek_key = g.grootboek_key" +
		" AND " + OrganisationClause +
		" AND g.gb_verdichting_code_1 = 4" +
		" AND g.gb_verdichting_code_2 = 45" +
		" AND f.month_key = 10" +
		" AND f.year_key = ?" +
		" GROUP BY g.gb_verdichting_toonnaam_1, g.gb_verdichting_toonnaam_2, g.gb_verdichting_toonnaam_3" +
		" ORDER BY g.gb_verdichting_toonnaam_1, g.gb_verdichting_toonnaam_2, g.gb_verdichting_toonnaam_3"		
	);
	
	public VoltTable run (int yearKey, int parentId) throws VoltAbortException {
		VoltTable[] queryResults;
						
		// get finance numbers
		long[] orgIds = this.getOrgIds(parentId);
		long min = orgIds[0];
		long max = orgIds[1];
		
		voltQueueSQL(GetNumbers, parentId, min, max, yearKey);		
		queryResults = voltExecuteSQL(true);
		
		VoltTable result = queryResults[0];
		
		return result;
	}

}
