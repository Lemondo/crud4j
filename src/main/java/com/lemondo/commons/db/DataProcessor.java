package com.lemondo.commons.db;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

public interface DataProcessor<T, L> {

	public Map<String, Object> bodyAsMap(T body);

	public T readRow(ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException;

	public L readAll(ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException;

	public void writeRows(OutputStream out, ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException;

}
