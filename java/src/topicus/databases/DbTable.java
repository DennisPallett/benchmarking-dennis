package topicus.databases;

import java.util.ArrayList;

public class DbTable {
	protected String name;
	protected ArrayList<DbColumn> columnList = new ArrayList<DbColumn>();
	
	protected String partitionBy = null;
	
	protected String orderBy = null;
	
	public DbTable (String name) {
		this.name = name;
	}
	
	public String getName () {
		return this.name;
	}
	
	public DbTable setName(String name) {
		this.name = name;
		return this;
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
	
	public DbTable setOrderBy(String colName) {
		this.orderBy = colName;
		return this;
	}
	
	public String orderBy () {
		return this.orderBy;
	}
	
	public String partitionBy() {
		return this.partitionBy;
	}
}
