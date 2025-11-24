package com.hazelcast.persistence.store;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 19:28
 **/
public abstract class PersistenceStorageStore<T extends PersistenceStorageAbstractConfig, R extends ExternalResource<T>> {
	protected PersistenceStorageAbstractConfig persistenceStorageAbstractConfig;
	protected ExternalResource<T> externalResource;
	protected AtomicBoolean enable = new AtomicBoolean(true);

	public void doInit(T t, R r) {
		this.persistenceStorageAbstractConfig = t;
		this.externalResource = r;
	}

	/**
	 * Do some light init operation, do not load data
	 */
	public void lightInit() {
		// do nothing
	}

	public void reInitResource(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
		// do nothing
	}

	/**
	 * Do some release operation, do not clear data
	 */
	public abstract void doDestroy();

	public void doClear() {
	}

	public void enable() {
		this.enable.compareAndSet(false, true);
	}

	public void disable() {
		this.enable.compareAndSet(true, false);
	}

	public PersistenceStorageAbstractConfig getPersistenceStorageAbstractConfig() {
		return persistenceStorageAbstractConfig;
	}

	public boolean checkEnable() {
		return this.enable.get();
	}

	public boolean configEquals(PersistenceStorageAbstractConfig config) {
		if (null == config && null == persistenceStorageAbstractConfig) {
			return true;
		} else if (null == config || null == persistenceStorageAbstractConfig) {
			return false;
		}
		return config.equals(persistenceStorageAbstractConfig);
	}

	public boolean isEmpty() {
		throw new UnsupportedOperationException();
	}

	public Map<String,Object> getStatistics() {
		throw new UnsupportedOperationException();
	}
}
