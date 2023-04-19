package com.hazelcast.persistence.http;

import com.tapdata.tm.sdk.available.CloudRestTemplate;
import com.tapdata.tm.sdk.interceptor.VersionHeaderInterceptor;
import com.tapdata.tm.sdk.util.CloudSignUtil;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-10-18 16:02
 **/
public class HttpUtil {

	public static RestTemplate getRestTemplate(int connectTimeout, int readTimeout) {
		RestTemplate restTemplate;
		if (CloudSignUtil.isNeedSign()) {
			restTemplate = new CloudRestTemplate(getClientHttpRequestFactory(connectTimeout, readTimeout));
		} else {
			restTemplate = new RestTemplate(getClientHttpRequestFactory(connectTimeout, readTimeout));
		}
		restTemplate.getMessageConverters().add(new TxMappingJackson2HttpMessageConverter());
		restTemplate.getInterceptors().add(new VersionHeaderInterceptor());
		return restTemplate;
	}

	private static ClientHttpRequestFactory getClientHttpRequestFactory(int connectTimeout, int readTimeout) {
		HttpComponentsClientHttpRequestFactory factory;
		try {
			TrustStrategy acceptingTrustStrategy = (x509Certificates, authType) -> true;
			SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
			SSLConnectionSocketFactory connectionSocketFactory =
					new SSLConnectionSocketFactory(sslContext, new NoopHostnameVerifier());

			HttpClientBuilder httpClientBuilder = HttpClients.custom()
							.setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
							.disableAutomaticRetries();
			httpClientBuilder.setSSLSocketFactory(connectionSocketFactory);
			CloseableHttpClient httpClient = httpClientBuilder.build();
			factory = new HttpComponentsClientHttpRequestFactory();
			factory.setHttpClient(httpClient);

			//Connect timeout
			factory.setConnectTimeout(connectTimeout <= 0 ? HttpConstant.DEFAULT_CONNECT_TIMEOUT : connectTimeout);

			//Read timeout
			factory.setReadTimeout(readTimeout <= 0 ? HttpConstant.DEFAULT_READ_TIMEOUT : readTimeout);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Create http request factory failed, message: %s", e.getMessage()), e);
		}
		return factory;
	}

	private static class TxMappingJackson2HttpMessageConverter extends MappingJackson2HttpMessageConverter {
		public TxMappingJackson2HttpMessageConverter() {
			List<MediaType> mediaTypes = new ArrayList<>();
			mediaTypes.add(MediaType.TEXT_PLAIN);
			mediaTypes.add(MediaType.TEXT_HTML);
			setSupportedMediaTypes(mediaTypes);
		}
	}
}
