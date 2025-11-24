package com.hazelcast.persistence.config;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import com.mongodb.ConnectionString;
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

	private boolean ssl;

	public PersistenceMongoDBConfig ssl(boolean ssl) {
		this.ssl = ssl;
		return this;
	}

	private String sslCA;

	public PersistenceMongoDBConfig sslCA(String sslCA) {
		this.sslCA = sslCA;
		return this;
	}

	private String sslKey;

	public PersistenceMongoDBConfig sslKey(String sslKey) {
		this.sslKey = sslKey;
		return this;
	}

	private String sslPass;

	public PersistenceMongoDBConfig sslPass(String sslPass) {
		this.sslPass = sslPass;
		return this;
	}

	private boolean sslValidate;

	public PersistenceMongoDBConfig sslValidate(boolean sslValidate) {
		this.sslValidate = sslValidate;
		return this;
	}

	private boolean checkServerIdentity;

	public PersistenceMongoDBConfig checkServerIdentity(boolean checkServerIdentity) {
		this.checkServerIdentity = checkServerIdentity;
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

	public boolean isSsl() {
		return ssl;
	}

	public String getSslCA() {
		return sslCA;
	}

	public String getSslKey() {
		return sslKey;
	}

	public String getSslPass() {
		return sslPass;
	}

	public boolean isSslValidate() {
		return sslValidate;
	}

	public boolean isCheckServerIdentity() {
		return checkServerIdentity;
	}

	public String maskUri() {
		if (StringUtils.isBlank(uri)) {
			return "";
		}
		ConnectionString connectionString = new ConnectionString(uri);
		char[] passwordCharArray = connectionString.getPassword();
		StringBuilder password = new StringBuilder();
		if (null != passwordCharArray) {
			for (char c : passwordCharArray) {
				password.append(c);
			}
		}
		String username = connectionString.getUsername();
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
			.add("ssl=" + ssl)
			.add("sslCA='" + sslCA + "'")
			.add("sslKey='" + sslKey + "'")
			.add("sslPass='" + sslPass + "'")
			.add("sslValidate=" + sslValidate)
			.add("checkServerIdentity=" + checkServerIdentity)
				.toString();
	}

	@Override
	public boolean equals(PersistenceStorageAbstractConfig config) {
		if (config instanceof PersistenceMongoDBConfig) {
			PersistenceMongoDBConfig mongoDBConfig = (PersistenceMongoDBConfig) config;
			return super.equals(config)
				&& equals(mongoDBConfig.getUri(), uri)
				&& equals(mongoDBConfig.getDatabase(), database)
				&& equals(mongoDBConfig.getCollection(), collection)
				&& mongoDBConfig.isSsl() == ssl
				&& equals(mongoDBConfig.getSslCA(), sslCA)
				&& equals(mongoDBConfig.getSslKey(), sslKey)
				&& equals(mongoDBConfig.getSslPass(), sslPass)
				&& mongoDBConfig.isSslValidate() == sslValidate
				&& mongoDBConfig.isCheckServerIdentity() == checkServerIdentity;
		}
		return super.equals(config);
	}

	private boolean equals(String v1, String v2) {
		if (null == v1) {
			return null == v2;
		}
		return v1.equals(v2);
	}

	public String uriInfo(){
		return maskUri() + "/" + database + "/" + collection;
	}
}
