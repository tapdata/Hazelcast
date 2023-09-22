package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.ttl.TTLConfig;
import com.hazelcast.persistence.store.ttl.TTLProcessorContext;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 17:37
 **/
public class RingBufferTTLProcessor extends BaseTTLProcessor {

	@Override
	public void doTTL(TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig) {
		long ttlSeconds = ttlConfig.getTtlSeconds();
		long ttlMillis = TimeUnit.SECONDS.toMillis(ttlSeconds);
		PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = ttlProcessorContext.getStore();
		if (!(store instanceof PersistenceRingBufferStore)) {
			return;
		}
		PersistenceRingBufferStore<?, ?> ringBufferStore = (PersistenceRingBufferStore<?, ?>) store;
		try {
			long t = ringBufferStore.getLargestSequence();
			if (t < 0) {
				return;
			}
			long s = ringBufferStore.getSmallestSequence() - 1;
			while (!Thread.currentThread().isInterrupted()) {
				s++;
				if (s >= t) {
					break;
				}
				Document document = null;
				try {
					Object obj = ringBufferStore.load(s);
					if (obj instanceof Document) {
						document = (Document) obj;
					}
				} catch (Exception e) {
					throw new RuntimeException("Read one from ringBuffer failed, sequence: " + s, e);
				}
				if (null == document) {
					continue;
				}
				if (document.containsKey("type") && "SIGN".equals(document.getString("type"))) {
					continue;
				}
				Long _ts = getTs(document);
				if (_ts == null) continue;
				if (System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(_ts) < ttlMillis) {
					break;
				}
				ringBufferStore.delete(s);
			}
		} catch (Exception e) {
			if (null != ttlProcessorContext.getLogger()) {
				ttlProcessorContext.getLogger().warn("Ringbuffer [{}] clear ttl data failed, ttl seconds: {}", ttlConfig.getPersistenceStorageAbstractConfig().getName(), ttlSeconds, e);
			}
		}
	}
}
