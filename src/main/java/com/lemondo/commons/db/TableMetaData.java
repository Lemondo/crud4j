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
		StringBuilder insertClause = new StringBuilder("INSERT INTO ").append(tableName).append(" (`id`");
		StringBuilder valuesClause = new StringBuilder(" VALUES").append(" (?");

		for (String col : columns) {
			if (columnDef.containsKey(col)) {
				insertClause.append(",`").append(col).append("`");
				valuesClause.append(",?");
			} else {
				throw new RuntimeException("BOOM: invalid field!");
			}
		}

		return insertClause.append(")").append(valuesClause).append(")").toString();
	}

	private String genUpdateSql(Set<String> columns) {
		StringBuilder updateSql = new StringBuilder("UPDATE ").append(tableName).append(" SET");

		String prefix = " ";
		for (String col : columns) {
			if (columnDef.containsKey(col)) {
				updateSql.append(prefix).append(col).append("=?");
				prefix = ",";
			} else {
				throw new RuntimeException("BOOM: invalid field!");
			}
		}

		return updateSql.append(" WHERE `id`=?").append(deactivatedFlag ? " AND `deactivated` = 0" : "").toString();
	}

	private String genDeleteSql() {
		StringBuilder deleteSql = new StringBuilder();

		if (deactivatedFlag) {
			deleteSql.append("UPDATE ").append(tableName).append("SET `deactivated`=1 WHERE `deactivated`=0 AND `id`=?");
		} else {
			deleteSql.append("DELETE FROM ").append(tableName).append(" WHERE `id`=?");
		}

		return deleteSql.toString();
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
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public CallableStatement prepareInsertStmnt(String key, Map<String, Object> body) {
		Set<String> columns = body.keySet();
		try {
			CallableStatement stmnt = helper.prepareCall(genInsertSql(columns));

			int i = 1;
			stmnt.setString(i++, key);
			for (String col : columns) {
				if (columnDef.containsKey(col)) {
					stmnt.setObject(i++, body.get(col), columnDef.get(col));
				} else {
					throw new RuntimeException("BOOM: invalid field!");
				}
			}

			return stmnt;
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public CallableStatement prepareUpdateStmnt(String key, Map<String, Object> body) {
		Set<String> columns = body.keySet();
		try {
			CallableStatement stmnt = helper.prepareCall(genUpdateSql(columns));

			int i = 1;
			for (String col : columns) {
				if (columnDef.containsKey(col)) {
					stmnt.setObject(i++, body.get(col), columnDef.get(col));
				} else {
					throw new RuntimeException("BOOM: invalid field!");
				}
			}
			stmnt.setString(i, key);

			return stmnt;
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

	@Override
	public CallableStatement prepareDeleteStmnt(String key) {
		try {
			CallableStatement stmnt = helper.prepareCall(genDeleteSql());

			stmnt.setString(1, key);

			return stmnt;
		} catch (SQLException e) {
			throw new RuntimeException("BOOM!", e);
		}
	}

}
