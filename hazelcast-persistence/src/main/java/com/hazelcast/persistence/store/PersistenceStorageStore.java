package com.hazelcast.persistence.store;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 19:28
 **/
public abstract class PersistenceStorageStore<T extends PersistenceStorageAbstractConfig, R extends ExternalResource<T>> {
	protected AtomicBoolean enable = new AtomicBoolean(true);

	public abstract void doInit(T t, R r);

	/**
	 * Do some release operation, do not clear data
	 */
	public abstract void doDestroy();

	public void enable() {
		this.enable.compareAndSet(false, true);
	}

	public void disable() {
		this.enable.compareAndSet(true, false);
	}

	protected boolean checkEnable() {
		return this.enable.get();
	}
}
