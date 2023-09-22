package com.hazelcast.persistence.store.ttl;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import org.apache.logging.log4j.Logger;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 18:49
 **/
public class TTLProcessorContext {
	private PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store;
	private Logger logger;

	public TTLProcessorContext(PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store, Logger logger) {
		this.store = store;
		this.logger = logger;
	}

	public PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> getStore() {
		return store;
	}

	public Logger getLogger() {
		return logger;
	}
}
