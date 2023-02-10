package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.external.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

import java.util.Optional;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class MongoDBRingBuffer extends PersistenceRingBufferStore<PersistenceMongoDBConfig, MongoDBResource> {
	private Long largestSequence = -1L;
	private Long smallestSequence = 0L;
	private Document sign;
	private MongoDBResource mongoDBResource;
	private PersistenceMongoDBConfig persistenceMongoDBConfig;

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
		this.destroy();
	}

	@Override
	public void destroy() {
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
		Document query = sign().append("key", sequence);
		Document doc = new Document(query).append("value", value.append("_ts", System.currentTimeMillis() / 1000));
		ReplaceOptions options = new ReplaceOptions().upsert(true);
		this.mongoDBResource.getMongoCollection().replaceOne(query, doc, options);
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
		Document query = sign().append("key", sequence);
		Document doc = this.mongoDBResource.getMongoCollection().find(query).first();
		if (doc == null) {
			return null;
		}
		return (Document) doc.get("value");
	}

	@Override
	public void delete(long s) {
		Document query = new Document(sign).append("key", s);
		try {
			this.mongoDBResource.getMongoCollection().deleteOne(query);
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
		return this._getSmallestSequence();
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