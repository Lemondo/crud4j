package com.lemondo.commons.db;

import java.util.HashMap;
import java.util.Map;

public class ProcMetaData {

	private final String procName;
	private final Map<String, Integer> paramDef;
	private final boolean returnsValue;
	private final Integer returnType;

	public ProcMetaData(String proc, Map<String, Integer> params) {
		this.procName = proc;
		this.paramDef = new HashMap<String, Integer>(params);
		this.returnsValue = false;
		this.returnType = 0;
	}

	public ProcMetaData(String proc, Map<String, Integer> params, int retType) {
		this.procName = proc;
		this.paramDef = new HashMap<String, Integer>(params);
		this.returnsValue = true;
		this.returnType = retType;
	}

	public String getProcName() {
		return procName;
	}

	public Map<String, Integer> getParamDef() {
		return new HashMap<String, Integer>(paramDef);
	}

	public Integer getReturnType() {
		return returnsValue ? returnType : null;
	}

	public String genProcedureCall() {
		StringBuilder result = new StringBuilder("CALL ");
		if (returnsValue) {
			result.append("? = ");
		}
		result.append("`").append(procName).append("`");
		if (paramDef != null && paramDef.size() > 0) {
			result.append("(?");
			for (int i = 1; i < paramDef.size(); i++) {
				result.append(",?");
			}
			result.append(")");
		}
		return result.toString();
	}
}
