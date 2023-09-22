package com.hazelcast.persistence.store.ttl;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 17:02
 **/
public class TTLConfig {
	private final PersistenceStorageAbstractConfig persistenceStorageAbstractConfig;
	private final long ttlSeconds;

	public TTLConfig(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig, long ttlSeconds) {
		this.persistenceStorageAbstractConfig = persistenceStorageAbstractConfig;
		this.ttlSeconds = ttlSeconds;
	}

	public PersistenceStorageAbstractConfig getPersistenceStorageAbstractConfig() {
		return persistenceStorageAbstractConfig;
	}

	public long getTtlSeconds() {
		return ttlSeconds;
	}
}
