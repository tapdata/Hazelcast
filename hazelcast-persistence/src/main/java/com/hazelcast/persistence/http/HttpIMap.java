package com.hazelcast.persistence.http;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.util.Properties;

/**
 * @author samuel
 * @Description
 * @create 2022-10-18 15:59
 **/
public abstract class HttpIMap extends Http implements MapStore<String, Object>, MapLoaderLifecycleSupport {
	protected String mapName;

	@Override
	public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
		initProperties(properties);
		restTemplate = HttpUtil.getRestTemplate(connectTimeout, readTimeout);
		this.mapName = mapName;
	}

	@Override
	public void destroy() {

	}
}
