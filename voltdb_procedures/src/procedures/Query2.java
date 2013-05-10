package procedures;

import org.voltdb.*;

public class Query2 extends VoltProcedure {
	public final SQLStmt GetResult = new SQLStmt(
			" SELECT" +
			" g.gb_verdichting_toonnaam_1," +
			" SUM( f.m_budgetbedrag ) AS begroting," +
			" SUM( f.m_realisatiebedrag ) AS realisatie" +
			" FROM fact_exploitatie AS f, dim_grootboek AS g, closure_organisatie AS c" +
			" WHERE f.grootboek_key = g.grootboek_key" +
			//" AND f.organisatie_key = c.organisatie_key" +
			//" AND c.parent = ?" +
			" AND (c.organisatie_key * 1000000 + c.parent) = (f.organisatie_key * 1000000 + ?) " +
			" AND (g.gb_verdichting_code_1 = 3 OR g.gb_verdichting_code_1 = 4 OR g.gb_verdichting_code_1 = 5)" +
			" AND f.month_key >= 06" +
			" AND f.month_key <= 11" +
			" AND f.year_key = ?" +
			" GROUP BY g.gb_verdichting_toonnaam_1" +
			" ORDER BY g.gb_verdichting_toonnaam_1",
			"fact_exploitatie,closure_organisatie,dim_grootboek"
	);
	
	public VoltTable run (int yearKey, int parentId) throws VoltAbortException {
		VoltTable[] queryResults;
		VoltTable result;
		
		voltQueueSQL(GetResult, parentId, yearKey); 
		queryResults = voltExecuteSQL(true);
		
		result = queryResults[0];		
		return result;
	}

}
