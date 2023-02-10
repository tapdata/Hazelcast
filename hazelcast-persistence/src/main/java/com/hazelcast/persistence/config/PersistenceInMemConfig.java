package com.hazelcast.persistence.config;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 15:19
 **/
public class PersistenceInMemConfig extends PersistenceStorageAbstractConfig {

	private PersistenceInMemConfig(ConstructType constructType, StorageMode storageMode) {
		super(constructType, storageMode);
	}

	public PersistenceInMemConfig(ConstructType constructType, StorageMode storageMode, String name) {
		super(constructType, storageMode, name);
	}

	public static PersistenceInMemConfig create(ConstructType constructType) {
		return new PersistenceInMemConfig(constructType, StorageMode.Mem);
	}

	public static PersistenceInMemConfig create(ConstructType constructType, String name) {
		return new PersistenceInMemConfig(constructType, StorageMode.Mem, name);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PersistenceInMemConfig.class.getSimpleName() + "[", "]")
				.add("constructType=" + constructType)
				.add("name='" + name + "'")
				.add("storageMode=" + storageMode)
				.add("inMemSize=" + inMemSize)
				.toString();
	}
}
