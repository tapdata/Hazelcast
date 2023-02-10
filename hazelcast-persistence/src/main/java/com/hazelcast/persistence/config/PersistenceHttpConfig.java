package com.hazelcast.persistence.config;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import com.hazelcast.persistence.http.HttpConstant;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 14:41
 **/
public class PersistenceHttpConfig extends PersistenceStorageAbstractConfig {

	private String baseUrl;

	private String accessCode;

	private PersistenceHttpConfig(ConstructType constructType, StorageMode storageMode, String baseUrl, String accessCode) {
		super(constructType, storageMode);
		this.baseUrl = baseUrl;
		this.accessCode = accessCode;
	}

	public PersistenceHttpConfig(ConstructType constructType, StorageMode storageMode, String name, String baseUrl, String accessCode) {
		super(constructType, storageMode, name);
		this.baseUrl = baseUrl;
		this.accessCode = accessCode;
	}

	public static PersistenceHttpConfig create(ConstructType constructType, String baseUrl, String accessCode) {
		return new PersistenceHttpConfig(constructType, StorageMode.HTTP_TM, baseUrl, accessCode);
	}

	public static PersistenceHttpConfig create(ConstructType constructType, String name, String baseUrl, String accessCode) {
		return new PersistenceHttpConfig(constructType, StorageMode.HTTP_TM, name, baseUrl, accessCode);
	}

	private Integer connectTimeoutMs = HttpConstant.DEFAULT_CONNECT_TIMEOUT;

	public PersistenceHttpConfig connectTimeoutMs(Integer connectTimeoutMs) {
		this.connectTimeoutMs = connectTimeoutMs;
		return this;
	}

	private Integer readTimeoutMs = HttpConstant.DEFAULT_READ_TIMEOUT;

	public PersistenceHttpConfig readTimeoutMs(Integer readTimeoutMs) {
		this.readTimeoutMs = readTimeoutMs;
		return this;
	}

	public String getBaseUrl() {
		return baseUrl;
	}

	public String getAccessCode() {
		return accessCode;
	}

	public Integer getConnectTimeoutMs() {
		return connectTimeoutMs;
	}

	public Integer getReadTimeoutMs() {
		return readTimeoutMs;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PersistenceHttpConfig.class.getSimpleName() + "[", "]")
				.add("constructType=" + constructType)
				.add("name='" + name + "'")
				.add("storageMode=" + storageMode)
				.add("inMemSize=" + inMemSize)
				.add("baseUrl='" + baseUrl + "'")
				.add("accessCode='******'")
				.add("connectTimeoutMs=" + connectTimeoutMs)
				.add("readTimeoutMs=" + readTimeoutMs)
				.toString();
	}

	@Override
	public boolean equals(PersistenceStorageAbstractConfig config) {
		if (config instanceof PersistenceHttpConfig) {
			return super.equals(config)
					&& ((PersistenceHttpConfig) config).getBaseUrl().equals(baseUrl)
					&& ((PersistenceHttpConfig) config).getAccessCode().equals(accessCode);
		}
		return super.equals(config);
	}
}
