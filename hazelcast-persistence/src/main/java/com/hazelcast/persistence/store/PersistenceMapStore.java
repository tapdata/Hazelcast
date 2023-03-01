package com.hazelcast.persistence.store;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;

import java.util.Properties;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 19:28
 **/
public abstract class PersistenceMapStore<T extends PersistenceStorageAbstractConfig, R extends ExternalResource<T>> extends PersistenceStorageStore<T, R>
		implements MapStore<String, Object>, MapLoaderLifecycleSupport {
	protected HazelcastInstance hazelcastInstance;
	protected String imapName;

	@Override
	public final void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
		// Replace with com.hazelcast.persistence.PersistenceStorageStore.doInit
		this.hazelcastInstance = hazelcastInstance;
	}

	@Override
	public void doInit(T t, R r) {
		super.doInit(t, r);
		this.imapName = t.getName();
	}
}
