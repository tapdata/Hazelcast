package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.collections4.map.LRUMap;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class MongoDBRingBuffer extends PersistenceRingBufferStore<PersistenceMongoDBConfig, MongoDBResource> {
	public static final int DEFAULT_FIND_LIMIT = 100;
	public static final int LRU_MAP_MAX_SIZE = DEFAULT_FIND_LIMIT + 1;
	private Long largestSequence = -1L;
	private Long smallestSequence = 0L;
	private Document sign;
	private MongoDBResource mongoDBResource;
	private PersistenceMongoDBConfig persistenceMongoDBConfig;
	private final LRUMap<String, Document> cacheMap = new LRUMap<>(LRU_MAP_MAX_SIZE);

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
		sign = new Document("ringBuffer", super.ringBufferName);
		this.largestSequence = this._getLargestSequence();
		this.smallestSequence = this._getSmallestSequence();
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
	}

	@Override
	public void store(long sequence, Document value) {
		if (!checkEnable()) {
			return;
		}
		if (sequence <= largestSequence) {
			return;
		}
		Document doc = getInsertDocument(sequence, value);
		this.mongoDBResource.getMongoCollection().insertOne(doc);
		this.largestSequence = sequence;
	}

	@Override
	public void storeAll(long l, Document[] values) {
		if (!checkEnable()) {
			return;
		}
		if (l <= largestSequence) {
			return;
		}
		List<WriteModel<Document>> models = new ArrayList<>();
		for (Document value : values) {
			Document doc = getInsertDocument(l++, value);
			models.add(new InsertOneModel<>(doc));
		}
		this.mongoDBResource.getMongoCollection().bulkWrite(models);
		this.largestSequence += l;
	}

	private Document getInsertDocument(long sequence, Document value) {
		return new Document(sign()).append("key", sequence).append("value", value.append("_ts", System.currentTimeMillis() / 1000));
	}

	@Override
	public Document load(long sequence) {
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
		if (!checkEnable()) {
			return;
		}
		Document query = sign().append("key", s);
		try {
			this.mongoDBResource.getMongoCollection().deleteOne(query);
			this.smallestSequence = _getSmallestSequence();
			this.largestSequence = _getLargestSequence();
		} catch (Exception e) {
			throw new RuntimeException("Delete from mongodb failed, query: " + query.toJson(), e);
		}
	}

	@Override
	public long getLargestSequence() {
		return this.largestSequence;
	}

	public long _getLargestSequence() {
		Document query = sign();
		Document doc = this.mongoDBResource.getMongoCollection().find(query).sort(descending("key")).first();
		if (doc == null) {
			return -1;
		}
		return doc.getLong("key");
	}

	@Override
	public long getSmallestSequence() {
		return this.smallestSequence;
	}

	public long _getSmallestSequence() {
		Document query = sign();
		Document doc = this.mongoDBResource.getMongoCollection().find(query).sort(ascending("key")).first();
		if (doc == null) {
			return 0;
		}
		return doc.getLong("key");
	}
}