package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.ttl.TTLCleanRuleBase;
import com.hazelcast.persistence.store.ttl.TTLConfig;
import com.hazelcast.persistence.store.ttl.TTLProcessorContext;
import org.apache.commons.collections4.CollectionUtils;

import java.lang.reflect.Method;
import java.util.*;
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
		List<TTLCleanRuleBase> ttlCleanRuleList = ttlConfig.getTtlCleanRuleList();
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
			checkTTLCleanRule(ttlCleanRuleList,iterator,keys,mapStore,ttlProcessorContext,ttlConfig);
			checkTTLByGlobalExpiration(ttlMillis,iterator,keys,mapStore,ttlProcessorContext,ttlConfig);
			if (CollectionUtils.isNotEmpty(keys)) {
				mapStore.deleteAll(keys);
				ttlProcessorContext.getLogger().info("TTL delete imap keys: {}, config: {}, persistence: {}", keys, ttlConfig, ttlProcessorContext.getStore().getPersistenceStorageAbstractConfig());
				keys.clear();
			}
		} catch (Exception e) {
			if (null != ttlProcessorContext.getLogger()) {
				ttlProcessorContext.getLogger().warn("IMap [{}] clear ttl data failed, ttl seconds: {}", ttlConfig.getPersistenceStorageAbstractConfig().getName(), ttlSeconds, e);
			}
		}
	}

	protected void checkTTLCleanRule(List<TTLCleanRuleBase> ttlCleanRuleList,Iterable<?> iterator,Set<String> keys,PersistenceMapStore<?, ?> mapStore,TTLProcessorContext ttlProcessorContext,TTLConfig ttlConfig){
		if(CollectionUtils.isNotEmpty(ttlCleanRuleList)){
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
						boolean isClean = (boolean)method.invoke(instance, map,ttlCleanRule);
						long ttlMillis = TimeUnit.SECONDS.toMillis(ttlCleanRule.getKeyTTLSeconds());
						if(isClean) cleanExpiredKey(map,ttlMillis,keys,map.get("key").toString(),mapStore,ttlProcessorContext,ttlConfig);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
			});
		}
	}

	protected void checkTTLByGlobalExpiration(long ttlMillis,Iterable<?> iterator,Set<String> keys,PersistenceMapStore<?, ?> mapStore,TTLProcessorContext ttlProcessorContext,TTLConfig ttlConfig){
		if(ttlMillis > 0){
			iterator.forEach(data -> {
				if (!(data instanceof Map)) {
					return;
				}
				Map<String, Object> map = (Map<String, Object>) data;
				Long _ts = getTs(map);
				if(null == _ts) {
					return ;
				}
				cleanExpiredKey(map,ttlMillis,keys,map.get("key").toString(),mapStore,ttlProcessorContext,ttlConfig);

			});
		}
	}

	protected void cleanExpiredKey(Map<String, Object> map,long ttlMillis,Set<String> keys,String key,PersistenceMapStore<?, ?> mapStore,TTLProcessorContext ttlProcessorContext,TTLConfig ttlConfig){
		Long _ts = getTs(map);
		if(null == _ts || ttlMillis <= 0) {
			return ;
		}
		if(System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(_ts) < ttlMillis){
			return;
		}
		keys.add(key);
		if (keys.size() == BATCH_SIZE) {
			mapStore.deleteAll(keys);
			keys.clear();
			ttlProcessorContext.getLogger().info("TTL delete imap keys: {}, config: {}, persistence: {}", keys, ttlConfig, ttlProcessorContext.getStore().getPersistenceStorageAbstractConfig());
		}
	}
}
