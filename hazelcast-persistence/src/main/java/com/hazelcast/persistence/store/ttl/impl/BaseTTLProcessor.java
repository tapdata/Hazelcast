package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.store.ttl.TTLProcessor;
import com.hazelcast.persistence.store.ttl.TTLTimestamps;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 18:06
 **/
public abstract class BaseTTLProcessor implements TTLProcessor {
	protected Long getTs(Map<String, Object> map) {
		return TTLTimestamps.read(map);
	}
}
