package procedures;

import org.voltdb.*;
import org.voltdb.VoltProcedure.VoltAbortException;

public class Query5 extends VoltProcedure {
	public final SQLStmt GetResult = new SQLStmt(
			" SELECT" +
			" o.id," +
			" o.naam," +
			" '(3) Baten'," +
			" SUM( f.m_budgetbedrag ) AS begroting," +
			" SUM( f.m_realisatiebedrag ) AS realisatie  " +
			" FROM fact_exploitatie AS f, dim_grootboek AS g, closure_organisatie AS c, organisatie AS o" +
			" WHERE f.grootboek_key = g.grootboek_key" +
			" AND f.organisatie_key = c.organisatie_key" +
			" AND c.parent = ?" +
			" AND o.id = c.parent" +
			" AND g.gb_verdichting_code_1 = 3" +
			" AND f.month_key >= 06" +
			" AND f.month_key <= 11" +
			" AND f.tenant_year_key = ?" +
			" GROUP BY o.id, o.naam" +
			" ORDER BY o.id, o.naam",
			"closure_organisatie,fact_exploitatie,dim_grootboek,organisatie"
	);
	
	public VoltTable run (int tenantYearKey, int parentId) throws VoltAbortException {
		VoltTable[] queryResults;
		VoltTable result;		

		voltQueueSQL(GetResult, parentId, tenantYearKey);
				
		queryResults = voltExecuteSQL(true);
		
		result = queryResults[0];	
		
		return result;
	}


}
