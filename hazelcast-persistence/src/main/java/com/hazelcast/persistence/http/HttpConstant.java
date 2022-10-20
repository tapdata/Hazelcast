package com.hazelcast.persistence.http;

/**
 * @author samuel
 * @Description
 * @create 2022-10-19 10:43
 **/
public class HttpConstant {
	public static final String BASE_URL_PROPERTY = "http.baseUrl";
	public static final String CONNECT_TIMEOUT_PROPERTY = "http.connectTimeout";
	public static final String READ_TIMEOUT_PROPERTY = "http.readTimeout";
	public static final String ACCESS_CODE_PROPERTY = "httpTM.accessCode";
	public final static int DEFAULT_CONNECT_TIMEOUT = 10_000;
	public final static int DEFAULT_READ_TIMEOUT = 30_000;
}
