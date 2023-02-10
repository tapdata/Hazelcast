package com.hazelcast.persistence.http;

import com.hazelcast.persistence.config.PersistenceHttpConfig;
import com.hazelcast.persistence.external.ExternalResource;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 23:32
 **/
public class HttpResource extends ExternalResource<PersistenceHttpConfig> {
	private String baseUrl;
	private int connectTimeout;
	private int readTimeout;
	private RestTemplate restTemplate;

	@Override
	public void doInit(PersistenceHttpConfig persistenceHttpConfig) {
		this.baseUrl = persistenceHttpConfig.getBaseUrl();
		if (StringUtils.isBlank(this.baseUrl)) {
			throw new IllegalArgumentException("Base url cannot be empty");
		}
		Object connectTimeoutObj = persistenceHttpConfig.getConnectTimeoutMs();
		if (connectTimeoutObj instanceof String) {
			this.connectTimeout = Integer.parseInt(connectTimeoutObj.toString());
		}
		Object readTimeoutObj = persistenceHttpConfig.getReadTimeoutMs();
		if (readTimeoutObj instanceof String) {
			this.readTimeout = Integer.parseInt(readTimeoutObj.toString());
		}
	}

	@Override
	public void close() throws IOException {
		this.baseUrl = null;
		this.restTemplate = null;
	}

	public String getBaseUrl() {
		return baseUrl;
	}

	public RestTemplate getRestTemplate() {
		return restTemplate;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public int getReadTimeout() {
		return readTimeout;
	}
}
