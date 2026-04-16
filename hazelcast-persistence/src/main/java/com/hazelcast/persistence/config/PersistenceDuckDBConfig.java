package com.hazelcast.persistence.config;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import org.apache.commons.lang3.StringUtils;

import java.util.StringJoiner;

public class PersistenceDuckDBConfig extends PersistenceStorageAbstractConfig {

	private PersistenceDuckDBConfig(ConstructType constructType, StorageMode storageMode) {
		super(constructType, storageMode);
	}

	public PersistenceDuckDBConfig(ConstructType constructType, StorageMode storageMode, String name) {
		super(constructType, storageMode, name);
	}

	public static PersistenceDuckDBConfig create(ConstructType constructType) {
		return new PersistenceDuckDBConfig(constructType, StorageMode.DuckDB);
	}

	public static PersistenceDuckDBConfig create(ConstructType constructType, String name) {
		return new PersistenceDuckDBConfig(constructType, StorageMode.DuckDB, name);
	}

	private String path;

	public PersistenceDuckDBConfig path(String path) {
		this.path = path;
		return this;
	}

	public String getPath() {
		return path;
	}

	public boolean isInMemory() {
		return StringUtils.isBlank(path);
	}

	public String jdbcUrl() {
		if (StringUtils.isBlank(path)) {
			return "jdbc:duckdb:";
		}
		if (path.startsWith("jdbc:duckdb:")) {
			return path;
		}
		return "jdbc:duckdb:" + path;
	}

	public String uriInfo() {
		return jdbcUrl();
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PersistenceDuckDBConfig.class.getSimpleName() + "[", "]")
				.add("constructType=" + constructType)
				.add("name='" + name + "'")
				.add("storageMode=" + storageMode)
				.add("inMemSize=" + inMemSize)
				.add("path='" + path + "'")
				.toString();
	}

	@Override
	public boolean equals(PersistenceStorageAbstractConfig config) {
		if (config instanceof PersistenceDuckDBConfig duckDBConfig) {
			if (!super.equals(config)) {
				return false;
			}
			if (path == null) {
				return duckDBConfig.getPath() == null;
			}
			return path.equals(duckDBConfig.getPath());
		}
		return super.equals(config);
	}
}
