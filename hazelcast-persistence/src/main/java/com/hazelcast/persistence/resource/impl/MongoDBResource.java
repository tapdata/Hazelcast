package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.io.IOException;

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

		this.mongoClient = MongoDBGlobalResource.getInstance().getMongoClient(persistenceMongoDBConfig);
		this.mongoDatabase = mongoClient.getDatabase(db);
		this.mongoCollection = mongoClient.getDatabase(db).getCollection(collection);
	}

	@Override
	public void close() throws IOException {
		MongoDBGlobalResource.getInstance().close(((PersistenceMongoDBConfig) persistenceStorageAbstractConfig).getUri());
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
