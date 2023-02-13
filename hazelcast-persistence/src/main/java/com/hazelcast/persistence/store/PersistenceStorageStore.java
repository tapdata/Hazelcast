package com.hazelcast.persistence.store;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.external.ExternalResource;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 19:28
 **/
public abstract class PersistenceStorageStore<T extends PersistenceStorageAbstractConfig, R extends ExternalResource<T>> {
	public abstract void doInit(T t, R r);

	/**
	 * Do some release operation, do not clear data
	 */
	public abstract void doDestroy();
}
