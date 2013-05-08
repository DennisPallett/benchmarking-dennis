package procedures;

import org.voltdb.*;

public class Query5 extends AbstractQuery {
	public final SQLStmt GetParent = new SQLStmt(
		"SELECT id, naam FROM organisatie WHERE id = ?"
	);
	
	public final SQLStmt GetNumbers = new SQLStmt(
		" SELECT" +
		" tenant_key," +
		" SUM( f.m_budgetbedrag ) AS begroting," +
		" SUM( f.m_realisatiebedrag ) AS realisatie  " +
		" FROM fact_exploitatie AS f, dim_grootboek AS g" +
		" WHERE f.grootboek_key = g.grootboek_key" +
		" AND " + OrganisationClause +
		" AND g.gb_verdichting_toonnaam_1 = '(3) Baten'" +
		" AND f.month_key >= 06" +
		" AND f.month_key <= 11" +
		" AND f.year_key = ?" +
		" GROUP BY tenant_key" +
		" ORDER BY tenant_key"		
	);
	
	public VoltTable run (int yearKey, int parentId) throws VoltAbortException {
		VoltTable[] queryResults;
		
		VoltTable result = new VoltTable(
                new VoltTable.ColumnInfo("id", VoltType.INTEGER),
                new VoltTable.ColumnInfo("naam", VoltType.STRING),
                new VoltTable.ColumnInfo("toonnaam", VoltType.STRING),
                new VoltTable.ColumnInfo("begroting", VoltType.FLOAT),
                new VoltTable.ColumnInfo("realisatie", VoltType.FLOAT));
				
		// get finance numbers
		long[] orgIds = this.getOrgIds(parentId);
		long min = orgIds[0];
		long max = orgIds[1];
		
		voltQueueSQL(GetNumbers, parentId, min, max, yearKey);
		
		// get parent info
		voltQueueSQL(GetParent, parentId);
		
		queryResults = voltExecuteSQL();
		
		VoltTable numbersTable = queryResults[0];
		VoltTable orgTable = queryResults[1];		
		
		if (orgTable.getRowCount() > 0) {
			String parentName = orgTable.fetchRow(0).getString("naam");
			
			double begroting = numbersTable.fetchRow(0).getDouble("begroting");
			double realisatie = numbersTable.fetchRow(0).getDouble("realisatie");
					
			result.addRow(new Object[] {
				parentId,
				parentName,
				"(3) Baten",
				begroting,
				realisatie
			});
		}
		
		return result;
	}

}
