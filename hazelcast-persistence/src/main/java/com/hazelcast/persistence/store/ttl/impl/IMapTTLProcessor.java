package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.ttl.TTLConfig;
import com.hazelcast.persistence.store.ttl.TTLProcessorContext;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 17:24
 **/
public class IMapTTLProcessor extends BaseTTLProcessor {

	public static final int BATCH_SIZE = 100;

	@Override
	public void doTTL(TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig) {
		long ttlSeconds = ttlConfig.getTtlSeconds();
		long ttlMillis = TimeUnit.SECONDS.toMillis(ttlSeconds);
		PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = ttlProcessorContext.getStore();
		if (!(store instanceof PersistenceMapStore)) {
			return;
		}
		PersistenceMapStore<?, ?> mapStore = (PersistenceMapStore<?, ?>) store;
		try {
			Iterable<?> iterator = mapStore.iterator();
			if (null == iterator) {
				return;
			}
			Set<String> keys = new HashSet<>();
			iterator.forEach(data -> {
				if (!(data instanceof Map)) {
					return;
				}
				Map<String, Object> map = (Map<String, Object>) data;
				Long _ts = getTs(map);
				if (null == _ts) {
					return;
				}
				if (System.currentTimeMillis() - TimeUnit.MILLISECONDS.toMillis(_ts) < ttlMillis) {
					return;
				}
				keys.add(map.get("key").toString());
				if (keys.size() == BATCH_SIZE) {
					mapStore.deleteAll(keys);
					keys.clear();
				}
			});
			if (CollectionUtils.isNotEmpty(keys)) {
				mapStore.deleteAll(keys);
				keys.clear();
			}
		} catch (Exception e) {
			if (null != ttlProcessorContext.getLogger()) {
				ttlProcessorContext.getLogger().warn("IMap [{}] clear ttl data failed, ttl seconds: {}", ttlConfig.getPersistenceStorageAbstractConfig().getName(), ttlSeconds, e);
			}
		}
	}
}
