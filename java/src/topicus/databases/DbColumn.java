package topicus.databases;

public class DbColumn {
	protected String name;
	
	protected Type type;
	protected int length;
	
	protected boolean isPrimaryKey = false;
	
	protected boolean isUnique = false;
	
	protected boolean isTenantKey = false;
	
	protected boolean allowNull = false;
	
	protected String foreignTable = null;
	protected String foreignColumn = null;
	
	public enum Type {INTEGER, SMALL_INTEGER, VARCHAR, TIMESTAMP, TIMESTAMP_WITHOUT_TIMEZONE, DOUBLE}	
	
	public DbColumn(String name) {
		this.name = name;
	}
	
	public String getName () {
		return this.name;
	}
	
	public Type getType () {
		return this.type;
	}
	
	public int getLength () {
		return this.length;
	}
	
	public DbColumn setType(Type type) {
		return this.setType(type, 0);
	}
	
	public DbColumn setType(Type type, int length) {
		this.type = type;
		this.length = length;
		return this;
	}
	
	public boolean isPrimaryKey () {
		return this.isPrimaryKey;
	}
		
	public DbColumn setPrimaryKey() {
		return this.setPrimaryKey(true);
	}
	
	public DbColumn setPrimaryKey(boolean value) {
		this.isPrimaryKey = value;
		return this;
	}
	
	public boolean allowNull () {
		return this.allowNull;
	}
	
	public boolean isTenantKey() {
		return this.isTenantKey;
	}
	
	public DbColumn setTenantKey() {
		return this.setTenantKey(true);
	}
	
	public DbColumn setTenantKey(boolean value) {
		this.isTenantKey = value;
		return this;
	}
	
	public DbColumn setAllowNull() {
		return this.setAllowNull(true);
	}
	
	public DbColumn setAllowNull(boolean value) {
		this.allowNull = value;
		return this;
	}
	
	public boolean isUnique () {
		return this.isUnique;
	}
	
	public DbColumn setUnique () {
		return this.setUnique(true);
	}
	
	public DbColumn setUnique (boolean value) {
		this.isUnique = value;
		return this;
	}
	
	public boolean isForeignKey () {
		return (this.foreignTable != null);
	}
	
	public String getForeignTable () {
		return this.foreignTable;
	}
	
	public String getForeignColumn () {
		return this.foreignColumn;
	}
	
	public DbColumn setForeignKey(String tableName, String colName) {
		this.foreignTable = tableName;
		this.foreignColumn = colName;
		return this;
	}

}
