package com.hazelcast.persistence.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.persistence.http.entity.IMapEntity;
import com.hazelcast.persistence.http.entity.ResponseBody;
import com.hazelcast.persistence.http.entity.TMRequestException;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author samuel
 * @Description
 * @create 2022-10-19 12:22
 **/
public abstract class Http {
	protected String baseUrl;
	protected int connectTimeout;
	protected int readTimeout;
	protected RestTemplate restTemplate;

	private final static String RETURN_TYPE_ARRAY = "array";
	private final static String RETURN_TYPE_OBJECT = "object";
	private final static String RETURN_TYPE_STRING = "string";

	public Http() {
	}

	protected void initProperties(Properties properties) {
		baseUrl = properties.getProperty(HttpConstant.BASE_URL_PROPERTY);
		if (StringUtils.isBlank(baseUrl)) {
			throw new IllegalArgumentException("Base url cannot be empty");
		}
		Object connectTimeoutObj = properties.get(HttpConstant.CONNECT_TIMEOUT_PROPERTY);
		if (connectTimeoutObj instanceof Integer) {
			connectTimeout = Integer.parseInt(connectTimeoutObj.toString());
		}
		Object readTimeoutObj = properties.get(HttpConstant.READ_TIMEOUT_PROPERTY);
		if (readTimeoutObj instanceof Integer) {
			readTimeout = Integer.parseInt(readTimeoutObj.toString());
		}
	}

	protected boolean successResp(ResponseEntity<ResponseBody> responseEntity) {
		if (responseEntity == null) {
			return false;
		}

		if (!responseEntity.hasBody()) {
			return false;
		}

		return responseEntity.getStatusCode().is2xxSuccessful() && ResponseCode.SUCCESS.getCode().equals(responseEntity.getBody().getCode());
	}

	protected <E> E post(URI uri, HttpEntity<?> httpEntity, TypeReference<E> typeReference) {
		ResponseEntity<ResponseBody> response = restTemplate.exchange(uri, HttpMethod.POST, httpEntity, ResponseBody.class);
		if (successResp(response)) {
			Object data = response.getBody().getData();
			if (null == data) {
				return null;
			}
			return JsonUtil.convertValue(data, typeReference);
		} else {
			throw new TMRequestException(String.format("Request post[%s] failed\n Request: %s\n Response: %s", uri, httpEntity, response));
		}
	}

	protected void upsert(Map<String, Object> param, HttpEntity<IMapEntity> httpEntity) {
		URI uri = getURI(param, Resource.HAZELCAST_PERSISTENCE, Resource.UPSERT_WITH_WHERE);
		ResponseEntity<ResponseBody> response = restTemplate.exchange(uri, HttpMethod.POST, httpEntity, ResponseBody.class);
		if (!successResp(response)) {
			throw new TMRequestException(String.format("Request upsert[%s] failed\n Request: %s\n Response: %s", uri, httpEntity, response));
		}
	}

	protected <E> List<E> find(Map<String, Object> param, TypeReference<E> typeReference) {
		URI uri = getURI(param, Resource.HAZELCAST_PERSISTENCE);
		ResponseEntity<ResponseBody> response = restTemplate.exchange(uri, HttpMethod.GET, null, ResponseBody.class);
		if (!successResp(response)) {
			return null;
		}
		Object data = response.getBody().getData();
		if (data instanceof Map && ((Map<?, ?>) data).containsKey("items")) {
			Object items = ((Map<?, ?>) data).get("items");
			if (items instanceof List) {
				List<E> retList = new ArrayList<>();
				((List<?>) items).forEach(obj -> retList.add(JsonUtil.convertValue(obj, typeReference)));
				return retList;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	protected <E> E findOne(Map<String, Object> param, TypeReference<E> typeReference) {
		URI uri = getURI(param, Resource.HAZELCAST_PERSISTENCE, Resource.FIND_ONE);
		ResponseEntity<ResponseBody> response = restTemplate.exchange(uri, HttpMethod.GET, null, ResponseBody.class);
		if (!successResp(response)) {
			return null;
		}
		Object data = response.getBody().getData();
		if (null == data) {
			return null;
		}
		return JsonUtil.convertValue(data, typeReference);
	}

	protected void delete(Map<String, Object> param) {
		URI uri = getURI(param, Resource.HAZELCAST_PERSISTENCE, Resource.DELETE_ALL);
		ResponseEntity<ResponseBody> response = restTemplate.exchange(uri, HttpMethod.DELETE, null, ResponseBody.class);
		if (!successResp(response)) {
			throw new TMRequestException(String.format("Request deleteAll[%s] failed\n Response: %s", uri, response));
		}
	}

	protected URI getURI(Resource... resources) {
		return getURI(null, resources);
	}

	protected URI getURI(Map<String, ?> params, Resource... resources) {
		StringBuilder url = new StringBuilder(baseUrl);
		if (resources != null) {
			for (Resource resource : resources) {
				url.append("/").append(resource.getResource());
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

	protected enum Resource {
		USER_GENERATE_TOKEN("users/generatetoken"),
		HAZELCAST_PERSISTENCE("HazelcastPersistence"),
		FIND_ONE("findOne"),
		UPSERT_WITH_WHERE("upsertWithWhere"),
		DELETE_ALL("deleteAll"),
		;
		private final String resource;

		Resource(String resource) {
			this.resource = resource;
		}

		public String getResource() {
			return resource;
		}
	}

	protected enum ResponseCode {
		SUCCESS("ok"),
		;

		private String code;

		ResponseCode(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}
	}
}
