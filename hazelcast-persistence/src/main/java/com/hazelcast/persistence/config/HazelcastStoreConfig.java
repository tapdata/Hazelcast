package com.hazelcast.persistence.config;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.ringbuffer.RingbufferStore;

/**
 * @author samuel
 * @Description
 * @create 2023-02-27 11:19
 **/
public class HazelcastStoreConfig<T> {
	private T hazelcastStoreConfig;

	public HazelcastStoreConfig(T hazelcastStoreConfig) {
		this.hazelcastStoreConfig = hazelcastStoreConfig;
	}

	public HazelcastStoreConfig<T> implementation(PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store) {
		if (null == store) {
			return this;
		}
		if (hazelcastStoreConfig instanceof MapStoreConfig) {
			((MapStoreConfig) hazelcastStoreConfig).setImplementation(store);
		} else if (hazelcastStoreConfig instanceof RingbufferStoreConfig) {
			((RingbufferStoreConfig) hazelcastStoreConfig).setStoreImplementation((RingbufferStore<?>) store);
		} else {
			throw new RuntimeException("Nonsupport hazelcast store config: " + hazelcastStoreConfig.getClass().getName());
		}
		return this;
	}
}
