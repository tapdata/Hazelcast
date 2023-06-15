package com.hazelcast.persistence.config;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 14:14
 **/
public abstract class PersistenceStorageAbstractConfig {
	protected ConstructType constructType;
	protected String name = "default";
	protected StorageMode storageMode;
	protected Integer inMemSize = 100;
	protected String maxSizePolicy;

	public PersistenceStorageAbstractConfig(ConstructType constructType, StorageMode storageMode) {
		this.constructType = constructType;
		this.storageMode = storageMode;
	}

	public PersistenceStorageAbstractConfig(ConstructType constructType, StorageMode storageMode, String name) {
		this.constructType = constructType;
		this.storageMode = storageMode;
		this.name = name;
	}

	public PersistenceStorageAbstractConfig(ConstructType constructType, String name, StorageMode storageMode, Integer inMemSize, String maxSizePolicy) {
		this.constructType = constructType;
		this.name = name;
		this.storageMode = storageMode;
		this.inMemSize = inMemSize;
		this.maxSizePolicy = maxSizePolicy;
	}

	public void setInMemSize(Integer inMemSize) {
		this.inMemSize = inMemSize;
	}

	public String getName() {
		return name;
	}

	public ConstructType getConstructType() {
		return constructType;
	}

	public StorageMode getStorageMode() {
		return storageMode;
	}

	public Integer getInMemSize() {
		return inMemSize;
	}

	public String getMaxSizePolicy() {
		return maxSizePolicy;
	}

	public void setMaxSizePolicy(String maxSizePolicy) {
		this.maxSizePolicy = maxSizePolicy;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PersistenceStorageAbstractConfig.class.getSimpleName() + "[", "]")
				.add("constructType=" + constructType)
				.add("name='" + name + "'")
				.add("storageMode=" + storageMode)
				.add("inMemSize=" + inMemSize)
				.add("maxSizePolicy=" + maxSizePolicy)
				.toString();
	}

	public boolean equals(PersistenceStorageAbstractConfig config) {
		return config.getName().equals(this.name)
				&& config.getStorageMode() == storageMode
				&& config.getConstructType() == constructType
				&& config.getInMemSize().equals(inMemSize);
	}
}
