package com.lemondo.commons.db.processor;

import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import com.lemondo.commons.db.exception.DataProcessingException;

public interface DataProcessor<T, L> {

	public Map<String, Object> bodyAsMap(T body) throws DataProcessingException;

	public T readRow(ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException, DataProcessingException;

	public L readAll(ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException, DataProcessingException;

	public void writeRows(OutputStream out, ResultSet rs, ResultSetMetaData rsmd, int numColumns) throws SQLException, DataProcessingException;

}
