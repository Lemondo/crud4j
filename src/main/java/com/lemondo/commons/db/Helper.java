package com.lemondo.commons.db;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class Helper {

	private final String dataSourceJndi;
	private final DataSource ds;

	private Connection conn;
	private List<CallableStatement> statementPool;

	public Helper(String dataSourceJndi) {
		this.dataSourceJndi = dataSourceJndi;
		try {
			this.ds = (DataSource) new InitialContext().lookup(this.dataSourceJndi);
		} catch (NamingException e) {
			throw new RuntimeException("BOOM!!");
		}
		this.statementPool = new ArrayList<CallableStatement>();
	}

	public String getDataSourceJndi() {
		return this.dataSourceJndi;
	}

	public Connection getConnection() throws SQLException {
		return (this.conn == null) ? (this.conn = ds.getConnection()) : this.conn;
	}

	public CallableStatement prepareCall(String sqlStatement) throws SQLException {
		CallableStatement stmnt = this.getConnection().prepareCall(sqlStatement);
		this.statementPool.add(stmnt);
		return stmnt;
	}

	private void close(Statement stmnt) {
		try {
			if (stmnt != null && !stmnt.isClosed()) {
				stmnt.close();
			}
		} catch (SQLException e) {
			System.err.println("WARNING: Error during finalization (possible memory leak): Cannot close Statement:");
			e.printStackTrace(System.err);
		}
	}

	private void close(Connection conn) {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			System.err.println("WARNING: Error during finalization (possible memory leak): Cannot close Connection:");
			e.printStackTrace(System.err);
		}
	}

	public void cleanup() {
		for (CallableStatement stmnt : this.statementPool) {
			close(stmnt);
		}
		close(this.conn);
	}

}
