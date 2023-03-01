package com.hazelcast.persistence;

import com.hazelcast.ringbuffer.RingbufferStore;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.collections4.map.LRUMap;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
	private AtomicLong largestSequence = new AtomicLong(-1L);
	private AtomicLong smallestSequence = new AtomicLong(0L);
	private Document sign;
	private final LRUMap<String, Document> cacheMap = new LRUMap<>(LRU_MAP_MAX_SIZE);
	private ScheduledExecutorService flushSequenceThreadPool;

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

		IndexOptions indexOptions = new IndexOptions().background(true);
		Bson keyIndex = Indexes.ascending("key", "ringBuffer");
		cacheCollection.createIndex(keyIndex, indexOptions);
		keyIndex = Indexes.ascending("value.timestamp");
		cacheCollection.createIndex(keyIndex, indexOptions);
		this.ringBufferName = s;
		sign = new Document("ringBuffer", this.ringBufferName);

		flushSequence();
		this.flushSequenceThreadPool = new ScheduledThreadPoolExecutor(1);
		this.flushSequenceThreadPool.scheduleAtFixedRate(this::flushSequence, 5L, 5L, TimeUnit.SECONDS);
	}

	private void flushSequence() {
		this.smallestSequence.set(this._getSmallestSequence());
		this.largestSequence.set(this._getLargestSequence());
	}

	@Override
	public void destroy() {
		Optional.ofNullable(this.flushSequenceThreadPool).ifPresent(ExecutorService::shutdownNow);
		Optional.ofNullable(this.mongoClient).ifPresent(MongoClient::close);
	}

	@Override
	public void store(long sequence, Document value) {
		if (sequence <= largestSequence.get()) {
			return;
		}
		Document doc = getInsertDocument(sequence, value);
		cacheCollection.insertOne(doc);
		this.largestSequence.set(sequence);
	}

	private Document getInsertDocument(long sequence, Document value) {
		return new Document(sign()).append("key", sequence).append("value", value.append("_ts", System.currentTimeMillis() / 1000));
	}

	@Override
	public void storeAll(long l, Document[] values) {
		if (l <= largestSequence.get()) {
			return;
		}
		List<WriteModel<Document>> models = new ArrayList<>();
		for (Document value : values) {
			Document doc = getInsertDocument(l++, value);
			models.add(new InsertOneModel<>(doc));
		}
		cacheCollection.bulkWrite(models);
		this.largestSequence.set(l);
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
		return this.largestSequence.get();
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
		return this.smallestSequence.get();
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