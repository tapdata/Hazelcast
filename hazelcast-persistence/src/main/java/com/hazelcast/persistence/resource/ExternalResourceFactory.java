package com.hazelcast.persistence.resource;

import com.hazelcast.persistence.StorageMode;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.impl.MongoDBResource;
import com.hazelcast.persistence.resource.impl.RocksDBResource;
import com.hazelcast.persistence.http.HttpResource;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 22:51
 **/
public class ExternalResourceFactory {
	public <E extends PersistenceStorageAbstractConfig> ExternalResource<E> createExternalResource(StorageMode storageMode) {
		ExternalResource<E> externalResource = null;
		switch (storageMode) {
			case MongoDB:
				externalResource = (ExternalResource<E>) new MongoDBResource();
				break;
			case RocksDB:
				externalResource = (ExternalResource<E>) new RocksDBResource();
				break;
			case HTTP_TM:
				externalResource = (ExternalResource<E>) new HttpResource();
				break;
			default:
				break;
		}
		return externalResource;
	}
}
