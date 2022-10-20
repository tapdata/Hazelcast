package com.hazelcast.persistence.http.entity;

/**
 * @author samuel
 * @Description
 * @create 2022-10-19 14:55
 **/
public class TMRequestException extends RuntimeException {
	public TMRequestException() {
		super();
	}

	public TMRequestException(String message) {
		super(message);
	}

	public TMRequestException(String message, Throwable cause) {
		super(message, cause);
	}

	public TMRequestException(Throwable cause) {
		super(cause);
	}

	protected TMRequestException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
