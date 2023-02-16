package com.hazelcast.persistence.resource;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;

import java.io.Closeable;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 20:59
 **/
public abstract class ExternalResource<T extends PersistenceStorageAbstractConfig> implements Closeable {
	protected PersistenceStorageAbstractConfig persistenceStorageAbstractConfig;

	public void doInit(T t) {
		this.persistenceStorageAbstractConfig = t;
	}
}
