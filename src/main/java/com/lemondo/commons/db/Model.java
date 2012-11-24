package com.lemondo.commons.db;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public interface Model<T> {

	public int create(String key, T body);

	public int update(String key, T body);

	public int delete(String key);

	public T read(String key);

	public List<T> list(Map<String, Object> options);

	public void list(OutputStream out, Map<String, Object> options);

}
