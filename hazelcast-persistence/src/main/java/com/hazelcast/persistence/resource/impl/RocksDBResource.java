package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.RocksDBInstance;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.io.IOException;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 21:00
 **/
public class RocksDBResource extends ExternalResource<PersistenceRocksDBConfig> {
	private RocksDB rocksDB;
	private ColumnFamilyHandle columnFamilyHandle;
	private String dbPath;
	private String columnFamilyName;

	static {
		RocksDB.loadLibrary();
	}

	@Override
	public void doInit(PersistenceRocksDBConfig persistenceRocksDBConfig) {
		super.doInit(persistenceRocksDBConfig);
		this.dbPath = persistenceRocksDBConfig.getPath();
		if (StringUtils.isBlank(dbPath)) {
			throw new IllegalArgumentException("RocksDB path cannot be blank");
		}

		this.columnFamilyName = persistenceRocksDBConfig.getName();
		if (StringUtils.isBlank(columnFamilyName)) {
			throw new IllegalArgumentException("RocksDB column family name cannot be blank");
		}

		this.columnFamilyHandle = RocksDBInstance.getColumnFamilyHandle(dbPath, columnFamilyName);

		this.rocksDB = RocksDBInstance.getInstance(dbPath);
	}

	@Override
	public void close() throws IOException {
		RocksDBInstance.close(dbPath);
		this.columnFamilyHandle = null;
		this.rocksDB = null;
	}

	public RocksDB getRocksDB() {
		return rocksDB;
	}

	public void removeColumnFamily() {
		RocksDBInstance.removeColumnFamily(dbPath, columnFamilyName);
	}

	public ColumnFamilyHandle getColumnFamilyHandle() {
		return columnFamilyHandle;
	}

	public String getColumnFamilyName() {
		return columnFamilyName;
	}

	public String getDbPath() {
		return dbPath;
	}
}
