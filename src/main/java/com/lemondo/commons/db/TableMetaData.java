package com.lemondo.commons.db;

import java.sql.CallableStatement;
import java.util.Map;
import java.util.Set;

public class TableMetaData implements ModelMetaData<Map<String, Object>> {

	private String tableName;
	private Map<String, Integer> columnDef;
	private boolean deactivatedFlag;

	public TableMetaData(String tableName, Map<String, Integer> columnDef, boolean deactivatedFlag) {
		super();
		this.tableName = tableName;
		this.columnDef = columnDef;
		this.deactivatedFlag = deactivatedFlag;
	}

	private String genSelectSql(Map<String, Object> options, boolean allRows) {
		// TODO Auto-generated method stub
		return null;
	}

	private String genInsertSql(Set<String> columns) {
		// TODO Auto-generated method stub
		return null;
	}

	private String genUpdateSql(Set<String> columns) {
		// TODO Auto-generated method stub
		return null;
	}

	private String genDeleteSql() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareSelectStmnt(String key, Map<String, Object> options) {
		throw new RuntimeException("Not inmpelented");
	}

	@Override
	public CallableStatement prepareInsertStmnt(String key, Map<String, Object> body) {
		throw new RuntimeException("Not inmpelented");
	}

	@Override
	public CallableStatement prepareUpdateStmnt(String key, Map<String, Object> body) {
		throw new RuntimeException("Not inmpelented");
	}

	@Override
	public CallableStatement prepareDeleteStmnt(String key) {
		throw new RuntimeException("Not inmpelented");
	}

}
