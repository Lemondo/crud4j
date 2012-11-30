package com.lemondo.commons.db;

import java.util.List;
import java.util.Map;

public class HashApiModel extends TableApiModel<Map<String, Object>, List<Map<String, Object>>> {

	public HashApiModel(Helper helper) {
		super(null, helper, new HashDataProcessor());
	}

	public HashApiModel(TableMetaData meta, Helper helper) {
		super(meta, helper, new HashDataProcessor());
	}

}
