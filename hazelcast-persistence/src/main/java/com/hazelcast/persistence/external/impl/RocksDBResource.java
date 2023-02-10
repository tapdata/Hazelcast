package com.hazelcast.persistence.external.impl;

import com.hazelcast.persistence.store.RocksDBInstance;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.external.ExternalResource;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.util.Optional;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 21:00
 **/
public class RocksDBResource extends ExternalResource<PersistenceRocksDBConfig> {
	private RocksDB rocksDB;

	@Override
	public void doInit(PersistenceRocksDBConfig persistenceRocksDBConfig) {
		super.doInit(persistenceRocksDBConfig);
		String dbPath = persistenceRocksDBConfig.getPath();
		if (StringUtils.isBlank(dbPath)) {
			throw new IllegalArgumentException("RocksDB path cannot be blank");
		}
		this.rocksDB = RocksDBInstance.getInstance(dbPath);
	}

	@Override
	public void close() throws IOException {
		Optional.ofNullable(this.rocksDB).ifPresent(RocksDB::close);
		this.rocksDB = null;
	}

	public RocksDB getRocksDB() {
		return rocksDB;
	}
}
