package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import org.apache.commons.collections4.map.LRUMap;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class MongoDBRingBuffer extends PersistenceRingBufferStore<PersistenceMongoDBConfig, MongoDBResource> {
	public static final int DEFAULT_FIND_LIMIT = 100;
	public static final int LRU_MAP_MAX_SIZE = DEFAULT_FIND_LIMIT + 1;
	public static final long PERIOD_MS = 500L;
	public static final long CLEAR_CACHE_MAP_PERIOD_MINUTE = 10L;
	private AtomicLong largestSequence = new AtomicLong(-1L);
	private AtomicLong smallestSequence = new AtomicLong(0L);
	private Document sign;
	private MongoDBResource mongoDBResource;
	private PersistenceMongoDBConfig persistenceMongoDBConfig;
	private Map<String, Document> cacheMap = new LRUMap<>(LRU_MAP_MAX_SIZE);
	private ScheduledThreadPoolExecutor flushSequenceScheduler;

	private Document sign() {
		return new Document(sign);
	}

	public MongoDBRingBuffer() {
	}

	@Override
	public void doInit(PersistenceMongoDBConfig persistenceMongoDBConfig, MongoDBResource mongoDBResource) {
		super.doInit(persistenceMongoDBConfig, mongoDBResource);
		this.mongoDBResource = mongoDBResource;
		this.persistenceMongoDBConfig = persistenceMongoDBConfig;
		createIndex();
		sign = new Document("ringBuffer", this.mongoDBResource.getMongoCollection().getNamespace().getCollectionName());
		flushSequence();
		this.flushSequenceScheduler = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Flush-MongoDB-Ringbuffer-Sequence-Scheduler-" + ringBufferName));
		this.flushSequenceScheduler.scheduleWithFixedDelay(() -> {
			try {
				this.flushSequence();
			} catch (Throwable ignored) {
			}
		}, PERIOD_MS, PERIOD_MS, TimeUnit.MILLISECONDS);
	}

	private void createIndex() {
		if (mongoDBResource == null) {
			return;
		}
		IndexOptions indexOptions = new IndexOptions().background(true);
		MongoCollection<Document> mongoCollection = mongoDBResource.getMongoCollection();
		Bson keyIndex = Indexes.ascending("ringBuffer", "key");
		mongoCollection.createIndex(keyIndex, indexOptions);
		keyIndex = Indexes.ascending("value.timestamp");
		mongoCollection.createIndex(keyIndex, indexOptions);
		keyIndex = Indexes.ascending("ringBuffer", "value.timestamp", "_id");
		mongoCollection.createIndex(keyIndex, indexOptions);
	}

	private void flushSequence() {
		this.smallestSequence.set(this._getSmallestSequence());
		this.largestSequence.set(this._getLargestSequence());
	}

	@Override
	public void doDestroy() {
		Optional.ofNullable(this.mongoDBResource).ifPresent(mr -> CommonUtils.handleWithError(
				() -> {
					mr.close();
					this.mongoDBResource = null;
				}, throwable -> {
					throw new RuntimeException("Close IMap[" + ringBufferName + "]'s MongoDB resource failed, config: " + persistenceMongoDBConfig, throwable);
				})
		);
		Optional.ofNullable(this.flushSequenceScheduler).ifPresent(f -> CommonUtils.ignoreAnyError(f::shutdownNow));
	}

	@Override
	public void store(long sequence, Object value) {
		if (!(value instanceof Document)) {
			return;
		}
		Document document = (Document) value;
		if (!checkEnable() || mongoDBResource == null) {
			return;
		}
		Document doc = getInsertDocument(sequence, document);
		this.mongoDBResource.getMongoCollection().insertOne(doc);
		this.largestSequence.set(sequence);
	}

	@Override
	public void storeAll(long l, Object[] values) {
		if (!checkEnable() || mongoDBResource == null) {
			return;
		}
		List<WriteModel<Document>> models = new ArrayList<>();
		for (Object value : values) {
			if (!(value instanceof Document)) {
				continue;
			}
			Document document = (Document) value;
			Document insertDocument = getInsertDocument(l++, document);
			models.add(new InsertOneModel<>(insertDocument));
		}
		BulkWriteResult bulkWriteResult = this.mongoDBResource.getMongoCollection().bulkWrite(models, new BulkWriteOptions().ordered(true));
		int insertedCount = bulkWriteResult.getInsertedCount();
		this.largestSequence.set(this.largestSequence.get() + insertedCount);
	}

	private Document getInsertDocument(long sequence, Document value) {
		return new Document(sign()).append("key", sequence).append("value", value.append("_ts", System.currentTimeMillis() / 1000));
	}

	@Override
	public Document load(long sequence) {
		if (mongoDBResource == null) {
			return null;
		}
		String sequenceStr = String.valueOf(sequence);
		if (!cacheMap.containsKey(sequenceStr)) {
			Document query = sign().append("key", new Document("$gte", sequence));
			try (
					MongoCursor<Document> iterator = this.mongoDBResource.getMongoCollection().find(query)
							.sort(Sorts.ascending("key"))
							.limit(DEFAULT_FIND_LIMIT).iterator()
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
	public void delete(long s) {
		if (!checkEnable() || mongoDBResource == null) {
			return;
		}
		Document query = sign().append("key", s);
		try {
			this.mongoDBResource.getMongoCollection().deleteOne(query);
			flushSequence();
		} catch (Exception e) {
			throw new RuntimeException("Delete from mongodb failed, query: " + query.toJson(), e);
		}
	}

	@Override
	public long getLargestSequence() {
		return this.largestSequence.get();
	}

	public long _getLargestSequence() {
		if (mongoDBResource == null) {
			return -1;
		}
		Document query = sign();
		Document doc = this.mongoDBResource.getMongoCollection().find(query).sort(descending("key")).first();
		if (doc == null) {
			return -1;
		}
		return doc.getLong("key");
	}

	@Override
	public long getSmallestSequence() {
		return this.smallestSequence.get();
	}

	@Override
	public long findSequenceByTimestamp(long timestamp) {
		if (mongoDBResource == null) {
			return 0L;
		}
		flushSequence();
		if (largestSequence.get() == -1L) {
			return 0L;
		}
		Document query = new Document(sign()).append("value.timestamp", new Document("$gte", timestamp));
		Document document = mongoDBResource.getMongoCollection().find(query).sort(ascending("_id")).first();
		if (document == null) {
			return largestSequence.get() + 1L;
		}
		return document.getLong("key");
	}

	public long _getSmallestSequence() {
		if (mongoDBResource == null) {
			return 0;
		}
		Document query = sign();
		Document doc = this.mongoDBResource.getMongoCollection().find(query).sort(ascending("key")).first();
		if (doc == null) {
			return 0;
		}
		return doc.getLong("key");
	}
}