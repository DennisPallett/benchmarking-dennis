package procedures;

import org.voltdb.*;
import org.voltdb.VoltProcedure.VoltAbortException;

public class Query4 extends VoltProcedure {
	public final SQLStmt GetResult = new SQLStmt(
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
			" AND f.tenant_year_key = ?" +
			" GROUP BY g.gb_verdichting_toonnaam_1, g.gb_verdichting_toonnaam_2, g.gb_verdichting_toonnaam_3" +
			" ORDER BY g.gb_verdichting_toonnaam_1, g.gb_verdichting_toonnaam_2, g.gb_verdichting_toonnaam_3"
			,
			"closure_organisatie,fact_exploitatie,dim_grootboek"
	);
	
	public VoltTable run (int tenantYearKey, int parentId) throws VoltAbortException {
		VoltTable[] queryResults;
						
		voltQueueSQL(GetResult, parentId, tenantYearKey);	
		queryResults = voltExecuteSQL(true);
		
		VoltTable result = queryResults[0];
		
		return result;
	}

}
