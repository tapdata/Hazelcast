package com.hazelcast.persistence;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import com.mongodb.client.model.Indexes;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MonogoDBIMap implements MapStore<String, Object>, MapLoaderLifecycleSupport {
	private MongoClient mongoClient;
	private MongoCollection<Document> cacheCollection;
	private final Long autoCreateIndexDocumentLimit = 5000000L;
	private final String defaultMongoUri = "mongodb://127.0.0.1";
	private final String defaultMongoDB = "cache";
	private final String defaultMongoCollection = "imap";
	private String imapName;
	private Document sign;

	private Document sign() {
		return new Document(sign);
	}

	public MonogoDBIMap() {
	}

	@Override
	public void init(HazelcastInstance hazelcastInstance, Properties properties, String s) {
		String mongoUri = properties.getProperty("mongo.uri");
		if (mongoUri == null) {
			mongoUri = defaultMongoUri;
		}
		String db = properties.getProperty("mongo.db");
		if (db == null) {
			db = defaultMongoDB;
		}
		String collection = properties.getProperty("mongo.collection");
		if (collection == null) {
			collection = defaultMongoCollection;
		}

		mongoClient = new MongoClient(new MongoClientURI(mongoUri));
		cacheCollection = mongoClient.getDatabase(db).getCollection(collection);

		Long cacheCollectionCount = cacheCollection.countDocuments();
		if (cacheCollectionCount > autoCreateIndexDocumentLimit) {
			throw new RuntimeException(String.format("mongo uri: %s, db: %s, collection: %s config as cache collection, but no index on key field, and because its document count is too many, %d: more than: %d, we stop auto create it, please manual create index with {\"key\":1}", mongoUri, db, collection, cacheCollectionCount, autoCreateIndexDocumentLimit));
		}
		Bson keyIndex = Indexes.ascending("key", "imap");
		cacheCollection.createIndex(keyIndex);
		this.imapName = s;
		sign = new Document("imap", this.imapName);
	}

	@Override
	public void destroy() {
		this.mongoClient.close();
	}

	public synchronized void store(String key, Object value) {
		if (value instanceof Document) {
			Document query = sign().append("key", key);
			Document doc = new Document(query).append("value", value);
			ReplaceOptions options = new ReplaceOptions().upsert(true);
			cacheCollection.replaceOne(query, doc, options);
		}
		// TODO 其余类型暂时不持久化
	}

	public synchronized void storeAll(Map<String, Object> map) {
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			store(entry.getKey(), entry.getValue());
		}
	}

	public synchronized void delete(String key) {
		cacheCollection.deleteOne(sign().append("key", key));
	}

	public synchronized void deleteAll(Collection<String> keys) {
		for (String key : keys) {
			delete(key);
		}
	}

	public synchronized Document load(String key) {
		Document query = sign().append("key", key);
		Document doc = cacheCollection.find(query).first();
		if (doc == null) {
			return null;
		}
		return (Document) doc.get("value");
	}

	public synchronized Map<String, Object> loadAll(Collection<String> keys) {
		Map<String, Object> result = new HashMap<>();
		for (String key : keys) {
			result.put(key, load(key));
		}
		return result;
	}

	public Iterable<String> loadAllKeys() {
		return null;
	}
}