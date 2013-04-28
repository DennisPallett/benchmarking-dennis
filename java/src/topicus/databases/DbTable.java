package topicus.databases;

import java.util.ArrayList;

public class DbTable {
	protected String name;
	protected ArrayList<DbColumn> columnList = new ArrayList<DbColumn>();
	
	protected String partitionBy = null;
	
	public DbTable (String name) {
		this.name = name;
	}
	
	public String getName () {
		return this.name;
	}
	
	public ArrayList<DbColumn> getColumns () {
		return this.columnList;
	}
	
	public DbTable addColumn(DbColumn column) {
		columnList.add(column);
		return this;
	}
	
	public DbTable setPartitionBy(String colName) {
		this.partitionBy = colName;
		return this;
	}
	
	public String partitionBy() {
		return this.partitionBy;
	}
}
