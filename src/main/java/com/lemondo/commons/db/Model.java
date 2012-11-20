package com.lemondo.commons.db;

import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Map;

public interface Model<T> {

	public int create(String key, T body) throws SQLException;

	public int update(String key, T body) throws SQLException;

	public int delete(String key) throws SQLException;

	public T read(String key) throws SQLException;

	public void list(OutputStream out, Map<String, ?> options) throws SQLException;

}
