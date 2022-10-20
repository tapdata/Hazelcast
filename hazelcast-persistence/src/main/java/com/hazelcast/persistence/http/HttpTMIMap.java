package com.hazelcast.persistence.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.StringCompression;
import com.hazelcast.persistence.http.entity.IMapEntity;
import com.hazelcast.persistence.http.entity.LoginResp;
import com.hazelcast.persistence.http.entity.TMRequestException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;

import java.net.URI;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-10-18 16:09
 **/
public class HttpTMIMap extends HttpIMap {
	private final static HttpHeaders headers = new HttpHeaders() {{
		add(HttpHeaders.CONTENT_TYPE, "application/json");
	}};
	private LoginResp loginResp;
	private String accessCode;

	@Override
	public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
		super.init(hazelcastInstance, properties, mapName);
		accessCode = properties.getProperty(HttpConstant.ACCESS_CODE_PROPERTY);
		if (StringUtils.isBlank(accessCode)) {
			throw new IllegalArgumentException("Access code cannot be empty");
		}
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
			put("where", param);
		}};
		return new HashMap<String, Object>() {{
			put("filter", where);
			put("access_token", loginResp.getId());
		}};
	}

	private Map<String, Object> whereQuery(Map<String, Object> param) {
		return new HashMap<String, Object>() {{
			put("where", param);
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
			put("key", new HashMap<String, Object>() {{
				put("$in", keys);
			}});
		}};
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
		Map<String, Object> params = new HashMap<>();
		params.put("accesscode", accessCode);
		HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(params, headers);
		URI uri = getURI(Resource.USER_GENERATE_TOKEN);
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
	}
}
