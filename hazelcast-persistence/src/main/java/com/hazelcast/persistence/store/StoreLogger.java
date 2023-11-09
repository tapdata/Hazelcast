package com.hazelcast.persistence.store;

/**
 * @author samuel
 * @Description
 * @create 2023-11-08 15:36
 **/
public interface StoreLogger {
	default void info(String message, Object... args) {
	}

	default void warn(String message, Object... args) {
	}

	default void error(String message, Object... args) {
	}

	default void debug(String message, Object... args) {
	}

	default boolean isDebugEnabled() {
		return false;
	}
}
