package com.hazelcast.persistence.store;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.http.HttpTMIMap;
import com.hazelcast.persistence.store.impl.MongoDBIMap;
import com.hazelcast.persistence.store.impl.MongoDBRingBuffer;
import com.hazelcast.persistence.store.impl.DuckDBIMap;
import com.hazelcast.persistence.store.impl.DuckDBRingBuffer;
import com.hazelcast.persistence.store.impl.RocksDBIMap;
import com.hazelcast.persistence.store.impl.RocksDBRingBuffer;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 23:02
 **/
public class PersistenceStoreFactory {
	public <E extends PersistenceStorageAbstractConfig, R extends ExternalResource<E>> PersistenceStorageStore<E, R> createStore(ConstructType constructType, StorageMode storageMode) {
		PersistenceStorageStore<E, R> persistenceStorageStore = null;
		switch (constructType) {
			case IMAP:
				switch (storageMode) {
					case MongoDB:
						persistenceStorageStore = (PersistenceStorageStore<E, R>) new MongoDBIMap();
						break;
					case RocksDB:
						persistenceStorageStore = (PersistenceStorageStore<E, R>) new RocksDBIMap();
						break;
					case DuckDB:
						persistenceStorageStore = (PersistenceStorageStore<E, R>) new DuckDBIMap();
						break;
					case HTTP_TM:
						persistenceStorageStore = (PersistenceStorageStore<E, R>) new HttpTMIMap();
						break;
				}
				break;
			case RINGBUFFER:
				switch (storageMode) {
					case MongoDB:
						persistenceStorageStore = (PersistenceStorageStore<E, R>) new MongoDBRingBuffer();
						break;
					case RocksDB:
						persistenceStorageStore = (PersistenceStorageStore<E, R>) new RocksDBRingBuffer();
						break;
					case DuckDB:
						persistenceStorageStore = (PersistenceStorageStore<E, R>) new DuckDBRingBuffer();
						break;
				}
				break;
		}
		return persistenceStorageStore;
	}
}
