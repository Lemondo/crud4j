package com.lemondo.commons.db.meta;

public class ProcParam {

	private final String name;
	private final int type;

	public ProcParam(String paramName, int paramType) {
		this.name = paramName;
		this.type = paramType;
	}

	public String getName() {
		return name;
	}

	public int getType() {
		return type;
	}

}
