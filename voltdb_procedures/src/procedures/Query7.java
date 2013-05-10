package procedures;

import org.voltdb.*;

public class Query7 extends VoltProcedure {
	public final SQLStmt GetNumbers = new SQLStmt(
		" SELECT" +
		" o.id," +
		" o.naam," +
		" '(4) Lasten'," +
		" '(45) Overige lasten'," +
		" SUM( f.m_budgetbedrag ) AS begroting," +
		" SUM( f.m_realisatiebedrag ) AS realisatie  " +
		" FROM fact_exploitatie AS f, dim_grootboek AS g, closure_organisatie AS c, organisatie AS o" +
		" WHERE f.grootboek_key = g.grootboek_key" +
		//" AND f.organisatie_key = c.organisatie_key" +
		//" AND c.parent = ?" +
		" AND (c.organisatie_key * 1000000 + c.parent) = (f.organisatie_key * 1000000 + ?) " +
		" AND o.id = c.parent" +
		" AND g.gb_verdichting_code_1 = 4" +
		" AND g.gb_verdichting_code_2 = 45" +
		" AND f.month_key = 10" +
		" AND f.year_key = ?" +
		" GROUP BY o.id, o.naam" +
		" ORDER BY o.id, o.naam",
		"fact_exploitatie,closure_organisatie,dim_grootboek,organisatie"	
	);
	
	public VoltTable run (int yearKey, int parentId) throws VoltAbortException {
		VoltTable[] queryResults;
		VoltTable result;		

		voltQueueSQL(GetNumbers, parentId, yearKey);
				
		queryResults = voltExecuteSQL(true);
		
		result = queryResults[0];	
		
		return result;
	}

}
