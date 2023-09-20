package com.hazelcast.persistence;

import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 21:55
 **/
public class CommonUtils {
	public static void ignoreAnyError(RunnableThrowAnyError runnable) {
		try {
			runnable.run();
		} catch (Throwable ignored) {
		}
	}

	public static void handleWithError(RunnableThrowAnyError runnable, Consumer<Throwable> errorConsumer) {
		try {
			runnable.run();
		} catch (Throwable throwable) {
			errorConsumer.accept(throwable);
		}
	}

	public interface RunnableThrowAnyError{
		void run() throws Throwable;
	}

	public static String getProperty(String key) {
		String value = System.getProperty(key);
		if(value == null)
			value = System.getenv(key);
		return value;
	}

	public static boolean getPropertyBool(String key, boolean defaultValue) {
		String value = System.getProperty(key);
		if(value == null)
			value = System.getenv(key);
		Boolean valueBoolean = null;
		if(value != null) {
			try {
				valueBoolean = Boolean.parseBoolean(value);
			} catch(Throwable ignored) {}
		}
		if(valueBoolean == null)
			valueBoolean = defaultValue;
		return valueBoolean;
	}

	public static int getPropertyInt(String key, int defaultValue) {
		String value = System.getProperty(key);
		if(value == null)
			value = System.getenv(key);
		Integer valueInt = null;
		if(value != null) {
			try {
				valueInt = Integer.parseInt(value);
			} catch(Throwable ignored) {}
		}
		if(valueInt == null)
			valueInt = defaultValue;
		return valueInt;
	}

	public static long getPropertyLong(String key, long defaultValue) {
		String value = System.getProperty(key);
		if(value == null)
			value = System.getenv(key);
		Long valueLong = null;
		if(value != null) {
			try {
				valueLong = Long.parseLong(value);
			} catch(Throwable ignored) {}
		}
		if(valueLong == null)
			valueLong = defaultValue;
		return valueLong;
	}
}
