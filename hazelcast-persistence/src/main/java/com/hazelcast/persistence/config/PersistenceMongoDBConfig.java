package com.hazelcast.persistence.config;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import com.mongodb.MongoClientURI;
import org.apache.commons.lang3.StringUtils;

import java.util.StringJoiner;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 14:18
 **/
public class PersistenceMongoDBConfig extends PersistenceStorageAbstractConfig {

	private PersistenceMongoDBConfig(ConstructType constructType, StorageMode storageMode) {
		super(constructType, storageMode);
	}

	public PersistenceMongoDBConfig(ConstructType constructType, StorageMode storageMode, String name) {
		super(constructType, storageMode, name);
	}

	public static PersistenceMongoDBConfig create(ConstructType constructType) {
		return new PersistenceMongoDBConfig(constructType, StorageMode.MongoDB);
	}

	public static PersistenceMongoDBConfig create(ConstructType constructType, String name) {
		return new PersistenceMongoDBConfig(constructType, StorageMode.MongoDB, name);
	}

	private String uri = "mongodb://127.0.0.1:27017";

	public PersistenceMongoDBConfig uri(String uri) {
		this.uri = uri;
		return this;
	}

	private String database = "hazelcast";

	public PersistenceMongoDBConfig database(String database) {
		this.database = database;
		return this;
	}

	private String collection = "tap_default_cache";

	public PersistenceMongoDBConfig collection(String collection) {
		this.collection = collection;
		return this;
	}

	private boolean exclusiveCollection;

	public PersistenceMongoDBConfig exclusiveCollection(boolean exclusiveCollection) {
		this.exclusiveCollection = exclusiveCollection;
		return this;
	}

	public String getUri() {
		return uri;
	}

	public String getDatabase() {
		return database;
	}

	public String getCollection() {
		return collection;
	}

	public boolean isExclusiveCollection() {
		return exclusiveCollection;
	}

	public String maskUri() {
		if (StringUtils.isBlank(uri)) {
			return "";
		}
		MongoClientURI mongoClientURI = new MongoClientURI(uri);
		char[] passwordCharArray = mongoClientURI.getPassword();
		StringBuilder password = new StringBuilder();
		if (null != passwordCharArray) {
			for (char c : passwordCharArray) {
				password.append(c);
			}
		}
		String username = mongoClientURI.getUsername();
		if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password.toString())) {
			return uri.replace(username + ":" + password, username + ":******");
		}
		return uri;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", PersistenceMongoDBConfig.class.getSimpleName() + "[", "]")
				.add("constructType=" + constructType)
				.add("name='" + name + "'")
				.add("storageMode=" + storageMode)
				.add("inMemSize=" + inMemSize)
				.add("uri='" + maskUri() + "'")
				.add("database='" + database + "'")
				.add("collection='" + collection + "'")
				.add("exclusiveCollection=" + exclusiveCollection)
				.toString();
	}

	@Override
	public boolean equals(PersistenceStorageAbstractConfig config) {
		if (config instanceof PersistenceMongoDBConfig) {
			return super.equals(config)
					&& ((PersistenceMongoDBConfig) config).getUri().equals(uri)
					&& ((PersistenceMongoDBConfig) config).getDatabase().equals(database)
					&& ((PersistenceMongoDBConfig) config).getCollection().equals(collection);
		}
		return super.equals(config);
	}
}
