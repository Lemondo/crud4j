package com.lemondo.commons.db.meta;

public class FilterCondition {
	private final String columnName;
	private final String operator;
	private final Object value;
	private final int type;

	public FilterCondition(String columnName, String operator, Object value, int type) {
		this.columnName = columnName;
		this.operator = operator;
		this.value = value;
		this.type = type;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getOperator() {
		return operator;
	}

	public Object getValue() {
		return value;
	}

	public int getType() {
		return type;
	}

}
