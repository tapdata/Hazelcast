package com.hazelcast.persistence.http;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;

import java.util.Properties;

/**
 * @author samuel
 * @Description
 * @create 2022-10-18 15:59
 **/
public abstract class HttpIMap<T extends PersistenceStorageAbstractConfig, R extends ExternalResource<T>> extends PersistenceStorageStore<T, R>
		implements MapStore<String, Object>, MapLoaderLifecycleSupport {
	protected HazelcastInstance hazelcastInstance;
	protected String mapName;

	@Override
	public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
		this.hazelcastInstance = hazelcastInstance;
		this.mapName = mapName;
	}
}
