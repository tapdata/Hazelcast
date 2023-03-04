package com.hazelcast.persistence;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

public class MongoDBIMap implements MapStore<String, Object>, MapLoaderLifecycleSupport {
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

	public MongoDBIMap() {
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

		mongoClient = MongodbUtil.createClient(mongoUri);
		cacheCollection = mongoClient.getDatabase(db).getCollection(collection);

		IndexOptions indexOptions = new IndexOptions().background(true);
		Bson keyIndex = Indexes.ascending("key", "imap");
		cacheCollection.createIndex(keyIndex, indexOptions);
		Bson tsIndex = Indexes.ascending("key", "ts");
		cacheCollection.createIndex(tsIndex, indexOptions);
		this.imapName = s;
		sign = new Document("imap", this.imapName);
	}

	@Override
	public void destroy() {
		this.mongoClient.close();
	}

	public synchronized void store(String key, Object value) {
		if (!(value instanceof Document)) {
			return;
		}
		value = ((Document) value).append("_ts", System.currentTimeMillis() / 1000);
		Document query = sign().append("key", key);
		Document doc = new Document(query).append("value", value);
		ReplaceOptions options = new ReplaceOptions().upsert(true);
		cacheCollection.replaceOne(query, doc, options);
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
		cacheCollection.deleteMany(new Document("imap", imapName));
//		for (String key : keys) {
//			delete(key);
//		}
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
		return new MongoDBImapIterable(cacheCollection.find(new Document("imap", imapName)));
	}

	class MongoDBImapIterable implements Iterable<String> {
		FindIterable<Document> mongoIterable;

		public MongoDBImapIterable(FindIterable<Document> mongoIterable) {
			if (null == mongoIterable) {
				throw new IllegalArgumentException("MongoIterable is null");
			}
			this.mongoIterable = mongoIterable;
		}

		@Override
		public void forEach(Consumer<? super String> action) {
			mongoIterable.forEach((Consumer<Document>) document -> action.accept(document.getString("key")));
		}

		@Override
		public Iterator<String> iterator() {
			return new MongoDBImapIterator(mongoIterable.iterator());
		}
	}

	class MongoDBImapIterator implements Iterator<String> {
		private MongoCursor<Document> mongoCursor;

		public MongoDBImapIterator(MongoCursor<Document> mongoCursor) {
			if (null == mongoCursor) {
				throw new IllegalArgumentException("MongoCursor is null");
			}
			this.mongoCursor = mongoCursor;
		}

		@Override
		public boolean hasNext() {
			return mongoCursor.hasNext();
		}

		@Override
		public String next() {
			return mongoCursor.next().getString("key");
		}
	}
}