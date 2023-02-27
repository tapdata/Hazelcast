package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.config.PersistenceInMemConfig;
import com.hazelcast.persistence.resource.ExternalResource;

import java.io.IOException;

/**
 * @author samuel
 * @Description
 * @create 2023-02-27 14:38
 **/
public class InMemoryResource extends ExternalResource<PersistenceInMemConfig> {
	@Override
	public void close() throws IOException {
		
	}
}
