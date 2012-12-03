package com.lemondo.commons.db.exception;

public class NotNullViolation extends DatabaseOperationException {
	private static final long serialVersionUID = -7919622254752421283L;

	public NotNullViolation() {
		super();
	}

	public NotNullViolation(String arg0) {
		super(arg0);
	}

	public NotNullViolation(Throwable arg0) {
		super(arg0);
	}

	public NotNullViolation(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
