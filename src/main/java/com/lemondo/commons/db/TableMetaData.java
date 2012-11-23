package com.lemondo.commons.db;

import java.sql.CallableStatement;
import java.util.HashSet;
import java.util.List;
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

	private class FilterCondition {
		String columnName;
		String operator;
		Object value;
		int type;

		FilterCondition(String columnName, String operator, Object value, int type) {
			this.columnName = columnName;
			this.operator = operator;
			this.value = value;
			this.type = type;
		}
	}

	private Set<FilterCondition> extractFilter(Map<String, Object> options) {
		Object filterRaw = options.get("filter");
		if (filterRaw == null) {
			return null;
		} else if (!(filterRaw instanceof Map)) {
			throw new RuntimeException("BOOM!");
		} else {
			Set<FilterCondition> filter = new HashSet<TableMetaData.FilterCondition>();

			@SuppressWarnings("unchecked")
			Map<String, Object> filterMap = (Map<String, Object>) filterRaw;

			Set<String> filterFields = filterMap.keySet();
			for (String field : filterFields) {
				if (columnDef.containsKey(field)) {
					filter.add(new FilterCondition(field, "=", filterMap.get(field), columnDef.get(field)));
				}
			}

			return filter;
		}
	}

	private String genFilterString(Set<FilterCondition> filterColumns) {
		StringBuilder result = new StringBuilder();

		String prefix = "";
		for (FilterCondition condition : filterColumns) {
			result.append(prefix).append("`").append(condition.columnName).append(condition.operator).append("?");
			prefix = " AND ";
		}

		return result.toString();
	}

	private String genOrderByString(List<String> sortFields) {
		if (sortFields == null) {
			throw new RuntimeException("BOOM!");
		} else {
			StringBuilder result = new StringBuilder();
			
			String prefix = " ORDER BY ";
			for (int i = 0; i < sortFields.size(); i++) {
				result.append(prefix).append("`").append(sortFields.get(i)).append("`");
				prefix = ",";
			}
			
			return result.toString();
		}
	}

	private String genSelectSql(Map<String, Object> options, boolean allRows, Set<FilterCondition> filter, List<String> sortFields) {
		StringBuilder selectSql = new StringBuilder("SELECT `id`");

		Set<String> columns = columnDef.keySet();
		for (String column : columns) {
			selectSql.append(",`").append(column).append("`");
		}

		selectSql.append(" FROM ").append(tableName);

		String filterPrefix = " WHERE ";
		if (deactivatedFlag) {
			selectSql.append(filterPrefix).append("`deactivated` = 0");
			filterPrefix = " AND ";
		}
		if (!allRows) {
			selectSql.append(filterPrefix).append("`id` = ?");
			filterPrefix = " AND ";
		}
		if (filter != null) {
			selectSql.append(filterPrefix).append(genFilterString(filter));
		}
		if (allRows && sortFields != null) {
			selectSql.append(genOrderByString(sortFields));
		}
		// TODO Add paging here

		return selectSql.toString();
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
