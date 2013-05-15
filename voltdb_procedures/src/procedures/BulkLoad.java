package procedures;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

import org.voltdb.*;

public class BulkLoad extends VoltProcedure {
	// 50 MB buffer
	public static final int BUFFER_SIZE = 10 * 1024 * 1024;
		
	public final SQLStmt Insert_Month_Names = new SQLStmt(
		"INSERT INTO month_names (month, month_name) VALUES (?, ?)"
	);
	
	public final SQLStmt Insert_Tijd_Data = new SQLStmt(
		"INSERT INTO dim_tijdtabel VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	);
	
	public final SQLStmt Insert_Administratie = new SQLStmt(
		"INSERT INTO dim_administratie VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	);
	
	public final SQLStmt Insert_Grootboek = new SQLStmt(
		"INSERT INTO dim_grootboek VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	);
	
	public final SQLStmt Insert_Kostenplaats = new SQLStmt(
		"INSERT INTO dim_kostenplaats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
	);
	
	public final SQLStmt Insert_Organisatie = new SQLStmt(
		"INSERT INTO organisatie VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	);
	
	public final SQLStmt Insert_Closure = new SQLStmt(
		"INSERT INTO closure_organisatie VALUES (?, ?)"
	);
	
	public final SQLStmt Insert_Fact = new SQLStmt(
		"INSERT INTO fact_exploitatie VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	);
	
	public VoltTable run (String table, String filename, int offset) throws VoltAbortException {	
		try {
			return this._run(table, filename, offset);
		} catch (Exception e) {
			throw new VoltAbortException(e.getMessage());
		}
	}
	
	public static void main (String[] args) {	
		try {
			//RandomAccessFile file = new RandomAccessFile(new File("C:\\Users\\Dennis\\Downloads\\testdata\\1\\gb_data.tbl"), "r");
			
			//String filename = "C:\\Users\\Dennis\\Downloads\\testdata\\1\\gb_data.tbl";
			String filename = "C:\\Users\\Dennis\\Documents\\GitHub\\benchmarking-dennis\\data\\base-data\\month_names.tbl";
			
			ArrayList<String[]> data = new ArrayList<String[]>();
			
			int offset = 0;
			while(offset > -1) {
				offset = loadData(data, filename, offset);
			}
			
			System.out.println(data.size());
			
			//loadData("C:\\Users\\Dennis\\Downloads\\testdata\\1\\gb_data.tbl", newOffset);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public static int loadData (ArrayList<String[]> data, String filename, int offset) throws Exception {
		RandomAccessFile file = new RandomAccessFile(new File(filename), "r");
		
		file.seek(offset);
				
		byte[] buffer = new byte[BUFFER_SIZE];
		int stat = file.read(buffer);		
		file.close();
				
		// find last newline
		int cutOff = -1;
		for(int i=buffer.length-1; i >= 0; i--) {
			if (buffer[i] == 10) {
				cutOff = i+1;
				break;
			}
		}
		
		if (stat == -1 || cutOff == -1) {
			return -1;
		}
		
		int newOffset = offset + cutOff;
				
		String rawData = new String(buffer);
				
		rawData = rawData.substring(0, rawData.lastIndexOf("\n"));
		rawData = rawData.trim();
		
		String[] lines = rawData.split("\n");
		
		int columnCount = -1;
		int lineNum = 0;
		for(String line : lines) {
			lineNum++;
			String[] split = line.split("#");
	            
			// ignore empty rows
			if (split.length == 0) continue;
			 
			// verify that this row has same amount of columns as every other row
			if (columnCount > -1 && columnCount != split.length) {
				 throw new Exception("Invalid column count " + split.length + " in line " + lineNum);
			}
			
			columnCount = split.length;
			 
			// change empty values and NULL's to actual null's
			for(int i=0; i < split.length; i++) {
            	if (split[i] == null || split[i].length() == 0 || split[i].toLowerCase().equals("null")) {
            		split[i] = null;
            	}
            }
			 
			data.add(split);
		}
		
		
		return newOffset;
	}
		
	protected VoltTable _run (String table, String filename, int offset) throws Exception {		
		long startTime = System.currentTimeMillis();
		
		table = table.toLowerCase();
		
		// load data
		ArrayList<String[]> data = new ArrayList<String[]>();		
		int newOffset = BulkLoad.loadData(data, filename, offset);
		
		// loop through rows
		Iterator<String[]> iter = data.iterator();
        while(iter.hasNext()) {
        	String[] split = iter.next();
        
            if (table.equals("month_names")) {
            	voltQueueSQL(Insert_Month_Names, split[0], split[1]);
            	
            } else if (table.equals("dim_tijdtabel")) {
            	voltQueueSQL(Insert_Tijd_Data, split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8], split[9], split[10], split[11], 
            			split[12], split[13], split[14], split[15], split[16], split[17], split[18], split[19], split[20], split[21], split[22], split[23], 
            			split[24], split[25], split[26], split[27], split[28], split[29], split[30], split[31], split[32], split[33], split[34], split[35]);
            
            } else if (table.equals("dim_administratie")) {
            	voltQueueSQL(Insert_Administratie, split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8]);
            	
            } else if (table.equals("dim_grootboek")) {
            	voltQueueSQL(Insert_Grootboek, split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8], split[9], split[10], split[11], 
            			split[12], split[13], split[14], split[15], split[16]);
            	
            } else if (table.equals("dim_kostenplaats")) {
            	voltQueueSQL(Insert_Kostenplaats, split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8]);
            	
            } else if (table.equals("organisatie")) {
            	voltQueueSQL(Insert_Organisatie, split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8], split[9], split[10]);
            	
            } else if (table.equals("closure_organisatie")) {
            	voltQueueSQL(Insert_Closure, split[0], split[1]);
            	
            } else if (table.equals("fact_exploitatie")) {
            	voltQueueSQL(Insert_Fact, split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7], split[8], split[9], split[10], split[11], 
            			split[12], split[13], split[14], split[15], split[16], split[17], split[18], split[19]);            	
            } else {
            	throw new Exception("Unknown table `" + table + "`");
            }
        }		
		
		VoltTable[] queryResults = voltExecuteSQL(true);
		
		VoltTable result = new VoltTable(
			new VoltTable.ColumnInfo("offset",VoltType.INTEGER),
			new VoltTable.ColumnInfo("rowcount",VoltType.INTEGER),
			new VoltTable.ColumnInfo("exectime",VoltType.INTEGER)
		);
					
		result.addRow(new Object[] {
			newOffset,
			data.size(),
			(System.currentTimeMillis() - startTime)
		});
		
		return result;
	}

}
