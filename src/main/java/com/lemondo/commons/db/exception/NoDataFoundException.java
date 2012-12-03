package com.lemondo.commons.db.exception;

public class NoDataFoundException extends Exception {
	private static final long serialVersionUID = 3913911388292113354L;

	public NoDataFoundException() {
		super();
	}

	public NoDataFoundException(String arg0) {
		super(arg0);
	}

	public NoDataFoundException(Throwable arg0) {
		super(arg0);
	}

	public NoDataFoundException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
