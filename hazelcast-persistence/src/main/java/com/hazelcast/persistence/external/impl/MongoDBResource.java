package com.hazelcast.persistence.external.impl;

import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.external.ExternalResource;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
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

		this.mongoClient = MongodbUtil.createClient(mongoUri);
		this.mongoDatabase = mongoClient.getDatabase(db);
		this.mongoCollection = mongoClient.getDatabase(db).getCollection(collection);

		Long cacheCollectionCount = this.mongoCollection.countDocuments();
		if (cacheCollectionCount > AUTO_CREATE_INDEX_DOCUMENT_LIMIT) {
			throw new RuntimeException(String.format("mongo uri: %s, db: %s, collection: %s config as cache collection, but no index on key field, and because its document count is too many, %d: more than: %d, we stop auto create it, please manual create index with {\"key\":1}",
					mongoUri, db, collection, cacheCollectionCount, AUTO_CREATE_INDEX_DOCUMENT_LIMIT));
		}
		IndexOptions indexOptions = new IndexOptions().background(true);
		Bson keyIndex = Indexes.ascending("key", "ringBuffer");
		this.mongoCollection.createIndex(keyIndex, indexOptions);
		keyIndex = Indexes.ascending("value.timestamp");
		this.mongoCollection.createIndex(keyIndex, indexOptions);
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
