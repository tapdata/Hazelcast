package com.hazelcast.persistence.store;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.external.ExternalResource;
import com.hazelcast.ringbuffer.RingbufferStore;
import org.bson.Document;

import java.util.Properties;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 19:43
 **/
public abstract class PersistenceRingBufferStore<T extends PersistenceStorageAbstractConfig, R extends ExternalResource<T>> extends PersistenceStorageStore<T, R>
		implements RingbufferStore<Document> {
	protected String ringBufferName;

	@Override
	public void doInit(T t, R r) {
		this.ringBufferName = t.getName();
	}

	abstract public void delete(long s);

	abstract public long getSmallestSequence();
}
