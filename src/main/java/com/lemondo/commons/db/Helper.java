package com.lemondo.commons.db;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public abstract class Helper {

	private List<CallableStatement> statementPool;

	protected Connection conn;

	protected Helper() {
		this.statementPool = new ArrayList<CallableStatement>();
	}

	private static class PlainJdbcHelper extends Helper {

		private final String uri;
		private final String userName;
		private final String password;

		private PlainJdbcHelper(String driverName, String uri, String userName, String password) {
			super();

			this.uri = uri;
			this.userName = userName;
			this.password = password;

			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("BOOM!", e);
			}
		}

		@Override
		public Connection getConnection() throws SQLException {
			return (this.conn == null) ? (this.conn = DriverManager.getConnection(uri, userName, password)) : this.conn;
		}

	}

	private static class DataSourceHelper extends Helper {

		private final DataSource ds;

		private DataSourceHelper(DataSource ds) {
			super();
			this.ds = ds;
		}

		private DataSourceHelper(String dataSourceJndi) {
			super();
			try {
				this.ds = (DataSource) new InitialContext().lookup(dataSourceJndi);
			} catch (NamingException e) {
				throw new RuntimeException("BOOM!", e);
			}
		}

		@Override
		public Connection getConnection() throws SQLException {
			return (this.conn == null) ? (this.conn = ds.getConnection()) : this.conn;
		}
	}

	public static Helper getInstance(DataSource ds) {
		return new DataSourceHelper(ds);
	}

	public static Helper getInstance(String dataSourceJndi) {
		return new DataSourceHelper(dataSourceJndi);
	}

	public static Helper getInstance(String driverName, String uri, String userName, String password) {
		return new PlainJdbcHelper(driverName, uri, userName, password);
	}

	public abstract Connection getConnection() throws SQLException;

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
