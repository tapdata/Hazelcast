package com.hazelcast.persistence.http;

import com.hazelcast.persistence.config.PersistenceHttpConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import org.apache.commons.collections4.MapUtils;
import org.apache.http.NoHttpResponseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 23:32
 **/
public class HttpResource extends ExternalResource<PersistenceHttpConfig> {

	private final Logger logger = LogManager.getLogger(HttpResource.class);
	private List<String> baseURLs;

	private String baseUrl;
	private int connectTimeout;
	private int readTimeout;
	private RestTemplate restTemplate;

	private Supplier<Long> getRetryTimeout;
	private int retryTime = 10;
	private long retryInterval = 500;

	private static final String ERR_MSG_FORMAT = "Failed to call rest api, msg %s.";


	@Override
	public void doInit(PersistenceHttpConfig persistenceHttpConfig) {
		this.baseURLs = persistenceHttpConfig.getBaseURLs();
		if (this.baseURLs == null || this.baseURLs.size() == 0) {
			throw new IllegalArgumentException("Base url cannot be empty");
		}
		this.baseUrl = this.baseURLs.get(0);
		Object connectTimeoutObj = persistenceHttpConfig.getConnectTimeoutMs();
		if (connectTimeoutObj instanceof String) {
			this.connectTimeout = Integer.parseInt(connectTimeoutObj.toString());
		}
		Object readTimeoutObj = persistenceHttpConfig.getReadTimeoutMs();
		if (readTimeoutObj instanceof String) {
			this.readTimeout = Integer.parseInt(readTimeoutObj.toString());
		}
		this.restTemplate = HttpUtil.getRestTemplate(connectTimeout, readTimeout);
		this.getRetryTimeout = () -> 30000L;
	}

	public List<String> getBaseURLs() {
		return baseURLs;
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

	@Override
	public void close() throws IOException {

	}

	static class RetryInfo {
		private final String reqId;
		private final long begin;
		private final long timeout;
		private long retries;
		private String baseURL;
		private String reqURL;
		private Object reqParams;
		private Exception lastError;

		public RetryInfo(String baseURL, long timeout) {
			this.baseURL = baseURL;
			this.timeout = timeout;
			this.begin = System.currentTimeMillis();
			this.reqId = UUID.randomUUID().toString();
		}

		void showParams(Object reqParams) {
			this.reqParams = reqParams;
		}

		String getURL(String resource) {
			this.reqURL = this.baseURL + resource;
			return this.reqURL;
		}

		protected URI getURI(String... resources) {
			return getURI(null, resources);
		}

		protected URI getURI(Map<String, ?> params, String... resources) {
			StringBuilder url = new StringBuilder(baseURL);
			if (resources != null) {
				for (String resource : resources) {
					url.append("/").append(resource);
				}
			}
			UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(url.toString());
			if (MapUtils.isNotEmpty(params)) {
				for (Map.Entry<String, ?> entry : params.entrySet()) {
					builder.queryParam(entry.getKey(), UriUtils.encode(String.valueOf(entry.getValue()), StandardCharsets.UTF_8));
				}
			}
			return builder.build(true).toUri();
		}
	}

	public <T> T retryWrap(TryFunc<T> func, Predicate<?> stop) {
		RetryInfo retryInfo = new RetryInfo(this.baseUrl, Optional.ofNullable(getRetryTimeout).map(Supplier::get).orElse(retryTime * retryInterval));
		do {
			try {
				T result = func.tryFunc(retryInfo);
				if (null != retryInfo.lastError) {
					logger.info("RestApi '{}' completed, use {}ms, retries {}"
									, retryInfo.reqId, System.currentTimeMillis() - retryInfo.begin, retryInfo.retries);
					this.baseUrl = retryInfo.baseURL; // Change it to an available URL
				}
				return result;
			} catch (HttpMessageConversionException | InterruptedException ignored) {
				break;
			} catch (Exception e) {
				boolean changeURL = true;
				if (e instanceof HttpClientErrorException) {
					// If the parameter is incorrect, no retry will be performed
					if (404 == ((HttpClientErrorException) e).getRawStatusCode()) {
						throw new RuntimeException(String.format(ERR_MSG_FORMAT, "not found url: " + retryInfo.reqURL), e);
					}
					if (405 == ((HttpClientErrorException) e).getRawStatusCode()) {
						throw new RuntimeException(String.format(ERR_MSG_FORMAT, "Please upgrade engine"), e);
					}
					if (405 == ((HttpClientErrorException) e).getRawStatusCode()) {
						throw new RuntimeException(String.format(ERR_MSG_FORMAT, "Please upgrade engine"), e);
					}
					if (405 == ((HttpClientErrorException) e).getRawStatusCode()) {
						throw new RuntimeException(String.format(ERR_MSG_FORMAT, "Please upgrade engine"), e);
					}
				} else {
					// 'NoHttpResponseException' may occur with multithreaded requests, There is no need to switch services
					Throwable ex = e;
					while (null != ex) {
						if ( ex instanceof NoHttpResponseException) {
							changeURL = false;
							break;
						}
						ex = ex.getCause();
					}
				}

				// Print the first exception message
				if (null == retryInfo.lastError) {
					logger.warn("RestApi '{}' failed, use {}ms, retryTime {}ms, retryInterval {}ms, reqURL: {}, reqParams: {}, error message: {}"
									, retryInfo.reqId, System.currentTimeMillis()- retryInfo.begin, retryInfo.timeout, retryInterval, retryInfo.reqURL, retryInfo.reqParams, e.getMessage(), e);
				}

				try {
					TimeUnit.MILLISECONDS.sleep(retryInterval);
				} catch (InterruptedException ignored) {
					break;
				}

				// Record retry information
				retryInfo.retries++;
				if (changeURL) {
					retryInfo.lastError = e;
					retryInfo.baseURL = changeBaseURLToNext(retryInfo.baseURL);
				}
			}
		} while (
						System.currentTimeMillis() < retryInfo.begin + retryInfo.timeout
										&& (null == stop || !stop.test(null))
		);

		if (null == retryInfo.lastError) {
			throw new RuntimeException(String.format(ERR_MSG_FORMAT, "no exception"));
		} else if (null != retryInfo.reqParams) {
			throw new RuntimeException(String.format(ERR_MSG_FORMAT,
							" data size " + (retryInfo.reqParams.toString().getBytes().length / 1024 / 1024) + "M,"
											+ " " + retryInfo.lastError.getMessage()), retryInfo.lastError);
		} else {
			throw new RuntimeException(String.format(ERR_MSG_FORMAT, retryInfo.lastError.getMessage()), retryInfo.lastError);
		}
	}

	private synchronized String changeBaseURLToNext(String baseURL) {
		int index = 0;

		for (int i = 0; i < this.baseURLs.size() - 1; i++) {
			if (baseURL.equals(baseURLs.get(i))) {
				index = i + 1;
				break;
			}
		}
		return baseURLs.get(index);
	}

	interface TryFunc<T> {
		T tryFunc(RetryInfo retryInfo) throws Exception;
	}
}
