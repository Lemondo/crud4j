package com.lemondo.commons.db.exception;

public class ForeignKeyViolation extends DatabaseOperationException {
	private static final long serialVersionUID = 8243518365349640347L;

	public ForeignKeyViolation() {
		super();
	}

	public ForeignKeyViolation(String arg0) {
		super(arg0);
	}

	public ForeignKeyViolation(Throwable arg0) {
		super(arg0);
	}

	public ForeignKeyViolation(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
