package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.ttl.TTLCleanRuleBase;
import com.hazelcast.persistence.store.ttl.TTLConfig;
import com.hazelcast.persistence.store.ttl.TTLMetrics;
import com.hazelcast.persistence.store.ttl.TTLProcessorContext;
import org.apache.commons.collections4.CollectionUtils;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 17:24
 **/
public class IMapTTLProcessor extends BaseTTLProcessor {

	public static final int BATCH_SIZE = 100;

	@Override
	public TTLMetrics doTTL(TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig) {
		TTLMetrics ttlMetrics = new TTLMetrics(ttlConfig.getPersistenceStorageAbstractConfig().getName());
		long ttlSeconds = ttlConfig.getTtlSeconds();
		long ttlMillis = TimeUnit.SECONDS.toMillis(ttlSeconds);
		long startMs = System.currentTimeMillis();
		try {
			List<TTLCleanRuleBase> ttlCleanRuleList = ttlConfig.getTtlCleanRuleList();
			PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = ttlProcessorContext.getStore();
			if (!(store instanceof PersistenceMapStore)) {
				return ttlMetrics;
			}
			PersistenceMapStore<?, ?> mapStore = (PersistenceMapStore<?, ?>) store;
			Iterable<?> iterator = mapStore.iterator();
			if (null == iterator) {
				return ttlMetrics;
			}
			Set<String> keys = new HashSet<>();
			int checkTTLCleanRule = checkTTLCleanRule(ttlCleanRuleList, iterator, keys, mapStore, ttlProcessorContext, ttlConfig);
			ttlMetrics.setDeleteCount(ttlMetrics.getDeleteCount() + checkTTLCleanRule);
			int globalExpiration = checkTTLByGlobalExpiration(ttlMillis, iterator, keys, mapStore, ttlProcessorContext, ttlConfig);
			ttlMetrics.setDeleteCount(ttlMetrics.getDeleteCount() + globalExpiration);
			if (CollectionUtils.isNotEmpty(keys)) {
				mapStore.deleteAll(keys);
				ttlMetrics.setDeleteCount(ttlMetrics.getDeleteCount() + keys.size());
				keys.clear();
			}
		} catch (Exception e) {
			ttlMetrics.setError(e);
		} finally {
			ttlMetrics.setCostMs(System.currentTimeMillis() - startMs);
		}
		return ttlMetrics;
	}

	protected int checkTTLCleanRule(List<TTLCleanRuleBase> ttlCleanRuleList, Iterable<?> iterator, Set<String> keys, PersistenceMapStore<?, ?> mapStore, TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig) {
		AtomicInteger deleteCount = new AtomicInteger();
		if (CollectionUtils.isNotEmpty(ttlCleanRuleList)) {
			ttlCleanRuleList.forEach(ttlCleanRule -> {
				iterator.forEach(data -> {
					if (!(data instanceof Map)) {
						return;
					}
					Map<String, Object> map = (Map<String, Object>) data;
					try {
						Class<?> clazz = Class.forName(ttlCleanRule.getType().getClazz());
						Method method = clazz.getMethod("isClean", Map.class, TTLCleanRuleBase.class);
						Object instance = clazz.newInstance();
						boolean isClean = (boolean) method.invoke(instance, map, ttlCleanRule);
						long ttlMillis = TimeUnit.SECONDS.toMillis(ttlCleanRule.getKeyTTLSeconds());
						if (isClean) {
							deleteCount.addAndGet(cleanExpiredKey(map, ttlMillis, keys, map.get("key").toString(), mapStore, ttlProcessorContext, ttlConfig));
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
			});
		}
		return deleteCount.get();
	}

	protected int checkTTLByGlobalExpiration(long ttlMillis, Iterable<?> iterator, Set<String> keys, PersistenceMapStore<?, ?> mapStore, TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig) {
		AtomicInteger deleteCount = new AtomicInteger();
		if (ttlMillis > 0) {
			iterator.forEach(data -> {
				if (!(data instanceof Map)) {
					return;
				}
				Map<String, Object> map = (Map<String, Object>) data;
				Long _ts = getTs(map);
				if (null == _ts) {
					return;
				}
				deleteCount.addAndGet(cleanExpiredKey(map, ttlMillis, keys, map.get("key").toString(), mapStore, ttlProcessorContext, ttlConfig));
			});
		}
		return deleteCount.get();
	}

	protected int cleanExpiredKey(Map<String, Object> map, long ttlMillis, Set<String> keys, String key, PersistenceMapStore<?, ?> mapStore, TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig) {
		int deleteCount = 0;
		Long _ts = getTs(map);
		if (null == _ts || ttlMillis <= 0) {
			return 0;
		}
		if (System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(_ts) < ttlMillis) {
			return 0;
		}
		keys.add(key);
		if (keys.size() == BATCH_SIZE) {
			mapStore.deleteAll(keys);
			deleteCount += keys.size();
			keys.clear();
			ttlProcessorContext.getLogger().info("TTL delete imap keys: {}, config: {}, persistence: {}", keys, ttlConfig, ttlProcessorContext.getStore().getPersistenceStorageAbstractConfig());
		}
		return deleteCount;
	}
}
