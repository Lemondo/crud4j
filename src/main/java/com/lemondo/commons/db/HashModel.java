package com.lemondo.commons.db;

import java.util.List;
import java.util.Map;

public class HashModel extends TableModel<Map<String, Object>, List<Map<String, Object>>> {

	public HashModel(TableMetaData meta, Helper helper) {
		super(meta, helper, new HashDataProcessor());
	}

}
