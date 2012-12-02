package com.lemondo.commons.db;

import java.sql.Types;

public enum PrimarykeyType {
	
	VARCHAR (Types.VARCHAR),
	INTEGER (Types.INTEGER),
	LONG (Types.BIGINT);

	public final int sqlType;
	
	private PrimarykeyType(int sqlType) {
		this.sqlType = sqlType;
	}
	
}
