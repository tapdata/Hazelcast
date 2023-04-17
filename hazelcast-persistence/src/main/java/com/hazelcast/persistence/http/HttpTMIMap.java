package com.hazelcast.persistence.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceHttpConfig;
import com.hazelcast.persistence.http.entity.IMapEntity;
import com.hazelcast.persistence.http.entity.LoginResp;
import com.hazelcast.persistence.http.entity.ResponseBody;
import com.hazelcast.persistence.http.entity.TMRequestException;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-10-18 16:09
 **/
public class HttpTMIMap extends HttpIMap<PersistenceHttpConfig, HttpResource> {
	private final static HttpHeaders headers = new HttpHeaders() {{
		add(HttpHeaders.CONTENT_TYPE, "application/json");
	}};
	private LoginResp loginResp;
	private String accessCode;
	private PersistenceHttpConfig persistenceHttpConfig;
	private HttpResource httpResource;

	@Override
	public void doInit(PersistenceHttpConfig persistenceHttpConfig, HttpResource httpResource) {
		super.doInit(persistenceHttpConfig, httpResource);
		this.persistenceHttpConfig = persistenceHttpConfig;
		this.httpResource = httpResource;
		this.accessCode = persistenceHttpConfig.getAccessCode();
		if (StringUtils.isBlank(accessCode)) {
			throw new IllegalArgumentException("Access code cannot be empty");
		}
	}

	@Override
	public void doClear() {
		this.deleteAll(null);
	}

	@Override
	public void doDestroy() {
		this.destroy();
	}

	@Override
	public void destroy() {
		Optional.ofNullable(httpResource).ifPresent(hr -> CommonUtils.ignoreAnyError(hr::close));
	}

	@Override
	public Object load(String key) {
		validateToken();
		Map<String, Object> param = new HashMap<String, Object>() {{
			put("imap", mapName);
			put("key", key);
		}};
		Map<String, Object> queryMap = filterQuery(param);
		IMapEntity iMapEntity = findOne(queryMap, new TypeReference<IMapEntity>() {
		});
		if (null == iMapEntity) {
			return null;
		}
		return iMapEntity.getData();
	}

	@Override
	public Map<String, Object> loadAll(Collection<String> keys) {
		validateToken();
		Map<String, Object> param = new HashMap<String, Object>() {{
			put("imap", mapName);
			put("key", new HashMap<String, Object>() {{
				put("$in", keys);
			}});
		}};
		Map<String, Object> queryMap = filterQuery(param);
		List<IMapEntity> iMapEntities = find(queryMap, new TypeReference<IMapEntity>() {
		});
		if (null == iMapEntities) {
			return null;
		}
		Map<String, Object> retMap = new HashMap<>();
		for (IMapEntity iMapEntity : iMapEntities) {
			retMap.put(iMapEntity.getKey(), iMapEntity.getData());
		}
		return retMap;
	}


	@Override
	public Iterable<String> loadAllKeys() {
		validateToken();
		Map<String, Object> param = new HashMap<String, Object>() {{
			put("imap", mapName);
		}};
		Map<String, Object> queryMap = filterQuery(param);
		List<IMapEntity> iMapEntities = find(queryMap, new TypeReference<IMapEntity>() {
		});
		List<String> keys = new ArrayList<>();
		iMapEntities.forEach(iMapEntity -> keys.add(iMapEntity.getKey()));
		return new HttpTMIterable(keys);
	}

	public static class HttpTMIterable implements Iterable<String> {
		private List<String> keys;

		public HttpTMIterable(List<String> keys) {
			this.keys = keys;
			if (null == this.keys) {
				this.keys = new ArrayList<>();
			}
		}

		@Override
		public Iterator<String> iterator() {
			return keys.iterator();
		}

		@Override
		public void forEach(Consumer<? super String> action) {
			keys.forEach(action);
		}

		@Override
		public Spliterator<String> spliterator() {
			return keys.spliterator();
		}
	}

	@Override
	public void store(String key, Object value) {
		validateToken();
		Map<String, Object> param = new HashMap<String, Object>() {{
			put("imap", mapName);
			put("key", key);
		}};
		Map<String, Object> queryMap = whereQuery(param);
		Map<String, Object> valueMap = getValue(value);
		IMapEntity iMapEntity = IMapEntity.create()
				.imap(mapName)
				.key(key)
				.value(valueMap)
				.serializeAndCompressValue();
		HttpEntity<IMapEntity> httpEntity = new HttpEntity<>(iMapEntity, headers);
		upsert(queryMap, httpEntity);
	}

	private static Map<String, Object> getValue(Object value) {
		Map<String, Object> rootValue = new HashMap<>();
		rootValue.put("_ts", System.currentTimeMillis() / 1000);
		rootValue.put("value", value);
		return rootValue;
	}

	private Map<String, Object> filterQuery(Map<String, Object> param) {
		HashMap<String, Object> where = new HashMap<String, Object>() {{
			try {
				put("where", JacksonUtil.toJson(param));
			} catch (JsonProcessingException ignore) {
			}
		}};
		return new HashMap<String, Object>() {{
			put("filter", where);
			put("access_token", loginResp.getId());
		}};
	}

	private Map<String, Object> whereQuery(Map<String, Object> param) {
		return new HashMap<String, Object>() {{
			try {
				put("where", JacksonUtil.toJson(param));
			} catch (JsonProcessingException ignore) {
			}
			put("access_token", loginResp.getId());
		}};
	}

	@Override
	public void storeAll(Map<String, Object> map) {
		map.forEach(this::store);
	}

	@Override
	public void delete(String key) {
		validateToken();
		if (null == key) {
			return;
		}
		Map<String, Object> param = new HashMap<String, Object>() {{
			put("imap", mapName);
			put("key", key);
		}};
		Map<String, Object> queryMap = whereQuery(param);
		delete(queryMap);
	}

	@Override
	public void deleteAll(Collection<String> keys) {
		validateToken();
		Map<String, Object> param = new HashMap<String, Object>() {{
			put("imap", mapName);
		}};
		if (null != keys) {
			param.put("key", new HashMap<String, Object>() {{
				put("$in", keys);
			}});
		}
		Map<String, Object> queryMap = whereQuery(param);
		delete(queryMap);
	}

	private void validateToken() {
		if (loginResp == null) {
			refreshToken();
		} else {
			long expiredTimestamp = loginResp.getExpiredTimestamp();
			if (expiredTimestamp - System.currentTimeMillis() <= 86400 * 1000) {
				refreshToken();
			}
		}
	}

	private void refreshToken() {
		this.httpResource.retryWrap((retryInfo) -> {
			Map<String, Object> params = new HashMap<>();
			params.put("accesscode", accessCode);
			HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(params, headers);
			URI uri = retryInfo.getURI(Resource.USER_GENERATE_TOKEN.getResource());
			try {
				loginResp = post(uri, httpEntity, new TypeReference<LoginResp>() {
				});
				if (null == loginResp) {
					throw new RuntimeException(String.format("Login response is null, uri: %s", uri));
				}
				loginResp.calcExpiredTimestamp();
			} catch (Throwable e) {
				if (e instanceof TMRequestException) {
					throw e;
				} else {
					throw new TMRequestException(String.format("Request uri[%s] failed, error: %s\n Request: %s", uri, e.getMessage(), httpEntity), e);
				}
			}
			return false;
		}, null);
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
		ResponseEntity<ResponseBody> response = this.httpResource.getRestTemplate().exchange(uri, HttpMethod.POST, httpEntity, ResponseBody.class);
		if (successResp(response)) {
			Object data = response.getBody().getData();
			if (null == data) {
				return null;
			}
			return JacksonUtil.convertValue(data, typeReference);
		} else {
			throw new TMRequestException(String.format("Request post[%s] failed\n Request: %s\n Response: %s", uri, httpEntity, response));
		}
	}

	protected void upsert(Map<String, Object> param, HttpEntity<IMapEntity> httpEntity) {
		this.httpResource.retryWrap((retryInfo) -> {
			URI uri = retryInfo.getURI(param, Resource.HAZELCAST_PERSISTENCE.getResource(), Resource.UPSERT_WITH_WHERE.getResource());
			ResponseEntity<ResponseBody> response = this.httpResource.getRestTemplate().exchange(uri, HttpMethod.POST, httpEntity, ResponseBody.class);
			if (!successResp(response)) {
				throw new TMRequestException(String.format("Request upsert[%s] failed\n Request: %s\n Response: %s", uri, httpEntity, response));
			}
			return false;
		}, null);
	}

	protected <E> List<E> find(Map<String, Object> param, TypeReference<E> typeReference) {
		return this.httpResource.retryWrap((retryInfo) -> {
			URI uri = retryInfo.getURI(param, Resource.HAZELCAST_PERSISTENCE.getResource());
			ResponseEntity<ResponseBody> response = this.httpResource.getRestTemplate().exchange(uri, HttpMethod.GET, null, ResponseBody.class);
			if (!successResp(response)) {
				return null;
			}
			Object data = response.getBody().getData();
			if (data instanceof Map && ((Map<?, ?>) data).containsKey("items")) {
				Object items = ((Map<?, ?>) data).get("items");
				if (items instanceof List) {
					List<E> retList = new ArrayList<>();
					((List<?>) items).forEach(obj -> retList.add(JacksonUtil.convertValue(obj, typeReference)));
					return retList;
				} else {
					return null;
				}
			} else {
				return null;
			}
		}, null);
	}

	protected <E> E findOne(Map<String, Object> param, TypeReference<E> typeReference) {
		return this.httpResource.retryWrap((retryInfo) -> {
			URI uri = retryInfo.getURI(param, Resource.HAZELCAST_PERSISTENCE.getResource(), Resource.FIND_ONE.getResource());
			ResponseEntity<ResponseBody> response = this.httpResource.getRestTemplate().exchange(uri, HttpMethod.GET, null, ResponseBody.class);
			if (!successResp(response)) {
				return null;
			}
			Object data = response.getBody().getData();
			if (null == data) {
				return null;
			}
			return JacksonUtil.convertValue(data, typeReference);
		}, null);
	}

	protected void delete(Map<String, Object> param) {
		this.httpResource.retryWrap((retryInfo) -> {
			URI uri = retryInfo.getURI(param, Resource.HAZELCAST_PERSISTENCE.getResource(), Resource.DELETE_ALL.getResource());
			ResponseEntity<ResponseBody> response = this.httpResource.getRestTemplate().exchange(uri, HttpMethod.DELETE, null, ResponseBody.class);
			if (!successResp(response)) {
				throw new TMRequestException(String.format("Request deleteAll[%s] failed\n Response: %s", uri, response));
			}
			return false;
		}, null);
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

	public PersistenceHttpConfig getPersistenceHttpConfig() {
		return persistenceHttpConfig;
	}
}
