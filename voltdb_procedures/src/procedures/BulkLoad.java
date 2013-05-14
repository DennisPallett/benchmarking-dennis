package procedures;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.voltdb.*;

public class BulkLoad extends VoltProcedure {
	public final SQLStmt Insert_Month_Names = new SQLStmt(
		"INSERT INTO month_names (month, month_name) VALUES (?, ?)"
	);
	
	public final SQLStmt Insert_Tijd_Data = new SQLStmt(
		"INSERT INTO dim_tijdtabel VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	);
	
	public VoltTable run (String table, String filename, int offset) throws VoltAbortException {	
		try {
			return this._run(table, filename, offset);
		} catch (Exception e) {
			throw new VoltAbortException(e.getMessage());
		}
	}
	
	protected VoltTable _run (String table, String filename, int offset) throws Exception {		
		table = table.toLowerCase();
		
		BufferedReader br = new BufferedReader(new FileReader(filename));
		
		StringBuilder sb = new StringBuilder();
		
        String line = br.readLine();
        while (line != null) {
            String[] split = line.split("#");
            
            // todo: fix nulls
            
            if (table.equals("month_names")) {
            	voltQueueSQL(Insert_Month_Names, split[0], split[1]);
            	
            } else if (table.equals("dim_tijdtabel")) {
            	voltQueueSQL(Insert_Tijd_Data, split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8], split[9], split[10], split[11], 
            			split[12], split[13], split[14], split[15], split[16], split[17], split[18], split[19], split[20], split[21], split[22], split[23], 
            			split[24], split[25], split[26], split[27], split[28], split[29], split[30], split[31], split[32], split[33], split[34], split[35]);
            	
            } else {
            	br.close();
            	throw new Exception("Unknown table `" + table + "`");
            }
            
            line = br.readLine();
        }		
        
		br.close();
		
		VoltTable[] queryResults = voltExecuteSQL(true);
		
		VoltTable result = new VoltTable(
			new VoltTable.ColumnInfo("offset",VoltType.INTEGER),
			new VoltTable.ColumnInfo("rowcount",VoltType.INTEGER)
		);
		
		int newOffset = 0;
		int rowcount = 0;
			
		result.addRow(new Object[] {
			newOffset,
			rowcount
		});
		
		return result;
	}

}
