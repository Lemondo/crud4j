package com.lemondo.commons.db;

import java.io.OutputStream;
import java.util.Map;

public interface Model<T, L> {

	public void create(Object key, T body);
	
	public Object create(T body);

	public int update(Object key, T body);

	public int delete(Object key);

	public T read(Object key);

	public L list(Map<String, Object> options);

	public void list(OutputStream out, Map<String, Object> options);

}
