package com.hazelcast.persistence.config;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 14:24
 **/
public class PersistenceRocksDBConfig extends PersistenceStorageAbstractConfig {

	private PersistenceRocksDBConfig(ConstructType constructType, StorageMode storageMode) {
		super(constructType, storageMode);
	}

	public PersistenceRocksDBConfig(ConstructType constructType, StorageMode storageMode, String name) {
		super(constructType, storageMode, name);
	}

	public static PersistenceRocksDBConfig create(ConstructType constructType) {
		return new PersistenceRocksDBConfig(constructType, StorageMode.RocksDB);
	}

	public static PersistenceRocksDBConfig create(ConstructType constructType, String name) {
		return new PersistenceRocksDBConfig(constructType, StorageMode.RocksDB, name);
	}

	private String path = "./tap_default_rocksdb_cache";

	public PersistenceRocksDBConfig path(String path) {
		this.path = path;
		return this;
	}

	public String getPath() {
		return path;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PersistenceRocksDBConfig.class.getSimpleName() + "[", "]")
				.add("constructType=" + constructType)
				.add("name='" + name + "'")
				.add("storageMode=" + storageMode)
				.add("inMemSize=" + inMemSize)
				.add("path='" + path + "'")
				.toString();
	}

	@Override
	public boolean equals(PersistenceStorageAbstractConfig config) {
		if (config instanceof PersistenceRocksDBConfig) {
			return super.equals(config)
					&& ((PersistenceRocksDBConfig) config).getPath().equals(path);
		}
		return super.equals(config);
	}
}
