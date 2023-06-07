package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.utils.SSLUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 20:59
 **/
public class MongoDBResource extends ExternalResource<PersistenceMongoDBConfig> {
	private final static Long AUTO_CREATE_INDEX_DOCUMENT_LIMIT = 5000000L;
	private MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	private MongoCollection<Document> mongoCollection;
	@Override
	public void doInit(PersistenceMongoDBConfig persistenceMongoDBConfig) {
		super.doInit(persistenceMongoDBConfig);
		String mongoUri = persistenceMongoDBConfig.getUri();
		if (StringUtils.isBlank(mongoUri)) {
			throw new IllegalArgumentException("MongoDB uri cannot be blank");
		}
		String db = persistenceMongoDBConfig.getDatabase();
		if (StringUtils.isBlank(db)) {
			db = new MongoClientURI(mongoUri).getDatabase();
		}
		if (StringUtils.isBlank(db)) {
			throw new IllegalArgumentException("MongoDB database cannot be blank");
		}
		String collection = persistenceMongoDBConfig.getCollection();
		if (StringUtils.isBlank(collection)) {
			throw new IllegalArgumentException("MongoDB collection cannot be blank");
		}

		MongoClientOptions.Builder builder = MongoClientOptions.builder();
		try {
			List<String> trustCertificates = null;
			if (persistenceMongoDBConfig.isSslValidate()) {
				trustCertificates = SSLUtil.retriveCertificates(persistenceMongoDBConfig.getSslCA());
			}
			List<String> certificates = SSLUtil.retriveCertificates(persistenceMongoDBConfig.getSslCA());
			String sslKey = SSLUtil.retrivePrivateKey(persistenceMongoDBConfig.getSslKey());
			if (StringUtils.isNotBlank(sslKey) && CollectionUtils.isNotEmpty(certificates)) {
				SSLContext sslContext = SSLUtil.createSSLContext(sslKey, certificates, trustCertificates, persistenceMongoDBConfig.getSslPass());
				builder.sslContext(sslContext);
				builder.sslEnabled(true);
				builder.sslInvalidHostNameAllowed(!persistenceMongoDBConfig.isCheckServerIdentity());
			}
		} catch (Exception e) {
			throw new RuntimeException(String.format("MongoDB set client ssl options failed: %s, config: %s", e.getMessage(), persistenceMongoDBConfig), e);
		}

		this.mongoClient = MongodbUtil.createClient(mongoUri, builder.build());
		this.mongoDatabase = mongoClient.getDatabase(db);
		this.mongoCollection = mongoClient.getDatabase(db).getCollection(collection);
	}

	@Override
	public void close() throws IOException {
		Optional.ofNullable(this.mongoClient).ifPresent(MongoClient::close);
		this.mongoClient = null;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}

	public MongoCollection<Document> getMongoCollection() {
		return mongoCollection;
	}
}
