package com.lemondo.commons.db;

import java.util.List;
import java.util.Map;

import com.lemondo.commons.db.meta.TableMetaData;
import com.lemondo.commons.db.processor.BasicDataProcessor;

public class BasicApiModel extends ApiModel<Map<String, Object>, List<Map<String, Object>>> {

	public BasicApiModel(Helper helper) {
		super(helper, new BasicDataProcessor());
	}

	public BasicApiModel(Helper helper, TableMetaData meta) {
		super(helper, new BasicDataProcessor(), meta);
	}

}
