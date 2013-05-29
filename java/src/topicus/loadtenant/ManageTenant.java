package topicus.loadtenant;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ManageTenant {
	protected Connection conn;
	
	public void deleteDataFromTable(Connection conn, String tableName, String tenantField, int tenantId)
			throws SQLException {
				
		PreparedStatement q = conn.prepareStatement("DELETE FROM " + tableName + " WHERE " + tenantField + " = ?");
		q.setInt(1,  tenantId);
		
		try {
			q.execute();
		} catch (SQLException e) {
			throw e;
		}
	}

	public void deleteDataFromClosure(Connection conn, int beginKey, int endKey)
			throws SQLException {
		PreparedStatement q = conn.prepareStatement("DELETE FROM closure_organisatie " +
				"WHERE organisatie_key >= ? AND organisatie_key <= ?");
		q.setInt(1,  beginKey);
		q.setInt(2,  endKey);
		
		try {
			q.execute();
		} catch (SQLException e) {
			throw e;
		}
	}
	
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
}
