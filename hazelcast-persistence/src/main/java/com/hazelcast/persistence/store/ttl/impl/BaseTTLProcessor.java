package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.store.ttl.TTLProcessor;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 18:06
 **/
public abstract class BaseTTLProcessor implements TTLProcessor {
	protected Long getTs(Map<String, Object> map) {
		if (null != map) {
			if (map.get("value") instanceof Map) {
				map = (Map<String, Object>) map.get("value");
			}

			if (map.containsKey("_ts")) {
				Object tsObj = map.get("_ts");
				if (tsObj instanceof Long) {
					return (Long) tsObj;
				} else if (tsObj instanceof Integer) {
					return Long.parseLong(tsObj.toString());
				}
			}
		}

		return null;
	}
}
