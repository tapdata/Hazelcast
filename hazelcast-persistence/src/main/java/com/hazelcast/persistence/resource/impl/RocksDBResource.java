package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.RocksDBInstance;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDB;

import java.io.IOException;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 21:00
 **/
public class RocksDBResource extends ExternalResource<PersistenceRocksDBConfig> {
	private RocksDB rocksDB;
	private String dbPath;

	static {
		RocksDB.loadLibrary();
	}

	@Override
	public void doInit(PersistenceRocksDBConfig persistenceRocksDBConfig) {
		super.doInit(persistenceRocksDBConfig);
		dbPath = persistenceRocksDBConfig.getPath();
		if (StringUtils.isBlank(dbPath)) {
			throw new IllegalArgumentException("RocksDB path cannot be blank");
		}
		this.rocksDB = RocksDBInstance.getInstance(dbPath);
	}

	@Override
	public void close() throws IOException {
		RocksDBInstance.close(dbPath);
		this.rocksDB = null;
	}

	public RocksDB getRocksDB() {
		return rocksDB;
	}
}
