package com.hazelcast.persistence.store;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.ringbuffer.RingbufferStore;
import org.bson.Document;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 19:43
 **/
public abstract class PersistenceRingBufferStore<T extends PersistenceStorageAbstractConfig, R extends ExternalResource<T>> extends PersistenceStorageStore<T, R>
		implements RingbufferStore<Object> {
	protected String ringBufferName;
	protected final static Document EMPTY_DOCUMENT = new Document();

	@Override
	public void doInit(T t, R r) {
		super.doInit(t, r);
		this.ringBufferName = t.getName();
	}

	abstract public void delete(long s);

	abstract public long getSmallestSequence();

	abstract public long findSequenceByTimestamp(long timestamp);
}
