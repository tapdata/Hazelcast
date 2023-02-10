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
}
