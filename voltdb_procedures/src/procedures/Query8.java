package procedures;

import org.voltdb.*;

public class Query8 extends VoltProcedure {
	public final SQLStmt GetResult = new SQLStmt("SELECT " +
			" f.year_key," +
			" f.month_key," +
			" m.month_name," +
			" f.year_key * 100 + f.month_key AS maand," +
			" m.month_name || ' ' || f.year_key AS maand_naam," +
			" '(3) Baten'," +
			" SUM(f.m_budgetbedrag) AS begroting," +
			" SUM(f.m_realisatiebedrag) AS realisatie" +
			" FROM fact_exploitatie AS f, month_names AS m, dim_grootboek AS g, closure_organisatie AS c" +
			" WHERE f.month_key=m.month" +
			" AND f.grootboek_key=g.grootboek_key" +
			//" AND f.organisatie_key = c.organisatie_key" +
			//" AND c.parent = ?" +
			" AND (c.organisatie_key * 1000000 + c.parent) = (f.organisatie_key * 1000000 + ?) " +
			" AND g.gb_verdichting_code_1 = 3" +
			" AND f.month_key >= 06" +
			" AND f.month_key <= 11" +
			" AND f.year_key = ?" +
			" GROUP BY f.year_key, f.month_key, m.month_name" +
			" ORDER BY f.year_key, f.month_key, m.month_name",
			"fact_exploitatie,closure_organisatie,dim_grootboek,month_names"
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
