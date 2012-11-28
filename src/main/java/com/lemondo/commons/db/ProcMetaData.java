package com.lemondo.commons.db;

import java.util.ArrayList;
import java.util.List;

public class ProcMetaData {

	private final String procName;
	private final List<ProcParam> paramDef;
	private final boolean returnsValue;
	private final Integer returnType;

	public ProcMetaData(String proc, List<ProcParam> params) {
		this.procName = proc;
		this.paramDef = new ArrayList<ProcParam>(params);
		this.returnsValue = false;
		this.returnType = null;
	}

	public ProcMetaData(String proc, List<ProcParam> params, int retType) {
		this.procName = proc;
		this.paramDef = new ArrayList<ProcParam>(params);
		this.returnsValue = true;
		this.returnType = retType;
	}

	public String getProcName() {
		return procName;
	}

	public List<ProcParam> getParamDef() {
		return new ArrayList<ProcParam>(paramDef);
	}

	public Integer getReturnType() {
		return returnsValue ? returnType : null;
	}

	public String genProcedureCall() {
		StringBuilder result = new StringBuilder();
		if (returnsValue) {
			result.append("{?=CALL ");
		} else {
			result.append("CALL ");
		}
		result.append("`").append(procName).append("`");
		if (paramDef != null && paramDef.size() > 0) {
			result.append("(?");
			for (int i = 1; i < paramDef.size(); i++) {
				result.append(",?");
			}
			result.append(")");
		}
		if (returnsValue) {
			result.append("}");
		}
		return result.toString();
	}
}
