package com.hazelcast.persistence.store.ttl;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 16:06
 **/
public interface TTLProcessor {

	void doTTL(TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig);
}
