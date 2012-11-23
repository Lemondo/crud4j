package com.lemondo.commons.db;

import java.sql.CallableStatement;
import java.util.Map;

public interface ModelMetaData<T> {

	public CallableStatement prepareSelectStmnt(String key, Map<String, Object> options);

	public CallableStatement prepareInsertStmnt(String key, T body);

	public CallableStatement prepareUpdateStmnt(String key, T body);

	public CallableStatement prepareDeleteStmnt(String key);

}
