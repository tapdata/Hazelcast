package com.hazelcast.persistence;

import com.hazelcast.persistence.store.ttl.impl.IMapTTLProcessor;
import com.hazelcast.persistence.store.ttl.impl.RingBufferTTLProcessor;
import com.hazelcast.persistence.store.ttl.TTLProcessor;

import java.util.function.Supplier;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 14:52
 **/
public enum ConstructType {
	IMAP(IMapTTLProcessor::new),
	RINGBUFFER(RingBufferTTLProcessor::new),
	;

	private Supplier<TTLProcessor> ttlProcessor;

	ConstructType() {
	}

	ConstructType(Supplier<TTLProcessor> ttlProcessor) {
		this.ttlProcessor = ttlProcessor;
	}

	public Supplier<TTLProcessor> getTtlProcessor() {
		return ttlProcessor;
	}
}
