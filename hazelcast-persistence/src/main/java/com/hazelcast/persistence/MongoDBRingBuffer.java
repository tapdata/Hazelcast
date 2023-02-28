package com.hazelcast.persistence;

import com.hazelcast.ringbuffer.RingbufferStore;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import org.apache.commons.collections4.map.LRUMap;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Properties;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class MongoDBRingBuffer implements RingbufferStore<Document> {
	public static final int DEFAULT_FIND_LIMIT = 100;
	public static final int LRU_MAP_MAX_SIZE = DEFAULT_FIND_LIMIT + 1;
	private MongoClient mongoClient;
	private MongoCollection<Document> cacheCollection;
	private final Long autoCreateIndexDocumentLimit = 5000000L;
	private final String defaultMongoUri = "mongodb://127.0.0.1";
	private final String defaultMongoDB = "cache";
	private final String defaultMongoCollection = "ringBuffer";
	private String ringBufferName;
	private Long largestSequence = -1L;
	private Long smallestSequence = 0L;
	private Document sign;
	private final LRUMap<String, Document> cacheMap = new LRUMap<>(LRU_MAP_MAX_SIZE);

	private Document sign() {
		return new Document(sign);
	}

	public MongoDBRingBuffer() {
	}

	@Override
	public void init(Properties properties, String s) {
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

		/*Long cacheCollectionCount = cacheCollection.countDocuments();
		if (cacheCollectionCount > autoCreateIndexDocumentLimit) {
			throw new RuntimeException(String.format("mongo uri: %s, db: %s, collection: %s config as cache collection, but no index on key field, and because its document count is too many, %d: more than: %d, we stop auto create it, please manual create index with {\"key\":1}", mongoUri, db, collection, cacheCollectionCount, autoCreateIndexDocumentLimit));
		}*/
		IndexOptions indexOptions = new IndexOptions().background(true);
		Bson keyIndex = Indexes.ascending("key", "ringBuffer");
		cacheCollection.createIndex(keyIndex, indexOptions);
		keyIndex = Indexes.ascending("value.timestamp");
		cacheCollection.createIndex(keyIndex, indexOptions);
		this.ringBufferName = s;
		sign = new Document("ringBuffer", this.ringBufferName);

		this.largestSequence = this._getLargestSequence();
		this.smallestSequence = this._getSmallestSequence();
	}

	@Override
	public void destroy() {
		mongoClient.close();
	}

	@Override
	public void store(long sequence, Document value) {
		Document query = sign().append("key", sequence);
		Document doc = new Document(query).append("value", value.append("_ts", System.currentTimeMillis() / 1000));
		ReplaceOptions options = new ReplaceOptions().upsert(true);
		cacheCollection.replaceOne(query, doc, options);
		if (sequence <= largestSequence) {
			return;
		}
		this.largestSequence = sequence;
	}

	@Override
	public void storeAll(long l, Document[] values) {
		for (Document value : values) {
			store(l, value);
			l = l + 1;
		}
	}

	@Override
	public Document load(long sequence) {
		String sequenceStr = String.valueOf(sequence);
		if (!cacheMap.containsKey(sequenceStr)) {
			Document query = sign().append("key", new Document("$gte", sequence));
			try (
					MongoCursor<Document> iterator = cacheCollection.find(query).sort(Sorts.ascending("key")).limit(DEFAULT_FIND_LIMIT).iterator()
			) {
				while (iterator.hasNext()) {
					Document document = iterator.next();
					if (!document.containsKey("key")) {
						continue;
					}
					Object key = document.get("key");
					cacheMap.put(key.toString(), document);
				}
			}
		}
		Document document = cacheMap.get(sequenceStr);
		if (null == document) {
			return null;
		}
		if (document.containsKey("value")) {
			return (Document) document.get("value");
		} else {
			return null;
		}
	}

	@Override
	public long getLargestSequence() {
		return this.largestSequence;
	}

	public long _getLargestSequence() {
		Document query = sign();
		Document doc = cacheCollection.find(query).sort(descending("key")).first();
		if (doc == null) {
			return -1;
		}
		return doc.getLong("key");
	}

	@Override
	public long getSmallestSequence() {
		return this._getSmallestSequence();
	}

	public long _getSmallestSequence() {
		Document query = sign();
		Document doc = cacheCollection.find(query).sort(ascending("key")).first();
		if (doc == null) {
			return 0;
		}
		return doc.getLong("key");
	}
}