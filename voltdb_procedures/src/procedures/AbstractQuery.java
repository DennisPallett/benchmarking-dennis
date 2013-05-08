package procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class AbstractQuery extends VoltProcedure {
	public final SQLStmt GetOrgIds = new SQLStmt("SELECT organisatie_key FROM closure_organisatie WHERE parent = ? AND organisatie_key <> ?" +
			" ORDER BY organisatie_key ASC");
	
	public final String OrganisationClause = "(f.organisatie_key = ? OR (f.organisatie_key >= ? AND f.organisatie_key <= ?))";

	protected long[] getOrgIds (int parentId) {		
		VoltTable[] queryResults;
		VoltTable result;
		
		voltQueueSQL(GetOrgIds, parentId, parentId); 
		queryResults = voltExecuteSQL();
		
		result = queryResults[0];
		
		long min = Integer.MAX_VALUE;
		long max = Integer.MIN_VALUE;		

		// collect child organisation keys
		for(int i=0; i < result.getRowCount(); i++) {
			long orgId = result.fetchRow(i).getLong("organisatie_key");
			
			if (orgId < min) min = orgId;
			if (orgId > max) max = orgId;			
		}		
		
		return new long[]{min, max};
	}
	
	// dummy run void
	public long run (int ignore, int ignore2, int ignore3, int ignore4) throws VoltAbortException {
		return -1;
	}
	

}
