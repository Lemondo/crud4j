package com.lemondo.commons.db;

class FilterCondition {
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
