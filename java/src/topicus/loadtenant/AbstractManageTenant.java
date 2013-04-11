package topicus.loadtenant;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractManageTenant {
	protected Connection conn;
	
	public abstract void deleteDataFromTable(String tableName, String tenantField, int tenantId) throws SQLException;
	public abstract void deleteDataFromClosure(int beginKey, int endKey) throws SQLException;
	
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
}
