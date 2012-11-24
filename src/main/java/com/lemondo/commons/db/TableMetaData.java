package com.lemondo.commons.db;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableMetaData implements ModelMetaData<Map<String, Object>> {

	private Helper helper;

	private String tableName;
	private Map<String, Integer> columnDef;
	private boolean deactivatedFlag;

	public TableMetaData(Helper helper, String tableName, Map<String, Integer> columnDef, boolean deactivatedFlag) {
		super();
		this.helper = helper;
		this.tableName = tableName;
		this.columnDef = columnDef;
		this.deactivatedFlag = deactivatedFlag;
	}

	public TableMetaData(String dataSourceJndi, String tableName, Map<String, Integer> columnDef, boolean deactivatedFlag) {
		this(new Helper(dataSourceJndi), tableName, columnDef, deactivatedFlag);
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

	private Set<FilterCondition> extractFilterFields(Map<String, Object> options) {
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

	private List<String> extractSortFields(Map<String, Object> options) {
		Object sortFieldsRaw = options.get("order");
		if (sortFieldsRaw == null) {
			return null;
		} else if (!(sortFieldsRaw instanceof List)) {
			throw new RuntimeException("BOOM!");
		} else {
			@SuppressWarnings("unchecked")
			List<String> sortFields = (List<String>) sortFieldsRaw;
			return sortFields;
		}
	}

	private String genFilterString(Set<FilterCondition> filter) {
		StringBuilder result = new StringBuilder();

		String prefix = "";
		for (FilterCondition condition : filter) {
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

	private String genSelectSql(boolean allRows, Set<FilterCondition> filter, List<String> sortFields) {
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
		Set<FilterCondition> filter = extractFilterFields(options);
		List<String> sortFields = extractSortFields(options);
		boolean allRows = key == null;
		
		try {
			CallableStatement stmnt = helper.prepareCall(genSelectSql(allRows, filter, sortFields));
			
			int i = 1;
			if (!allRows) {
				stmnt.setString(i++, key);
			}
			
			for (FilterCondition condition : filter) {
				stmnt.setObject(i++, condition.value, condition.type);
			}
			// TODO Add paging here

			return stmnt;
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!!", e);
		}
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
