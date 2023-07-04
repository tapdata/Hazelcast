package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.function.Consumer;

public class MongoDBIMap extends PersistenceMapStore<PersistenceMongoDBConfig, MongoDBResource> {
	protected static final int STORE_ALL_BATCH_SIZE = 1000;
	protected static final int QUERY_BATCH_SIZE = 20;
	protected Document sign;
	private MongoDBResource mongoDBResource;
	private PersistenceMongoDBConfig persistenceMongoDBConfig;

	private Document sign() {
		return new Document(sign);
	}

	public MongoDBIMap() {
	}

	@Override
	public void doInit(PersistenceMongoDBConfig persistenceMongoDBConfig, MongoDBResource mongoDBResource) {
		super.doInit(persistenceMongoDBConfig, mongoDBResource);
		this.mongoDBResource = mongoDBResource;
		this.persistenceMongoDBConfig = persistenceMongoDBConfig;
		createIndex();
		this.sign = new Document("imap", super.imapName);
	}

	private void createIndex() {
		IndexOptions indexOptions = new IndexOptions().background(true);
		MongoCollection<Document> mongoCollection = mongoDBResource.getMongoCollection();
		Bson keyIndex = Indexes.ascending("key", "imap");
		mongoCollection.createIndex(keyIndex, indexOptions);
		Bson tsIndex = Indexes.ascending("key", "ts");
		mongoCollection.createIndex(tsIndex, indexOptions);
	}

	@Override
	public void doClear() {
		if (this.persistenceMongoDBConfig.isExclusiveCollection()) {
			mongoDBResource.getMongoCollection().drop();
		} else {
			this.deleteAll(null);
		}
	}

	@Override
	public void doDestroy() {
		releaseResource();
	}

	@Override
	public void destroy() {
		if (this.persistenceMongoDBConfig.isExclusiveCollection()) {
			mongoDBResource.getMongoCollection().drop();
		} else {
			this.deleteAll(null);
		}
		releaseResource();
	}

	private synchronized void releaseResource() {
		Optional.ofNullable(this.mongoDBResource).ifPresent(mr -> CommonUtils.handleWithError(
				() -> {
					mr.close();
					this.mongoDBResource = null;
				}, throwable -> {
					throw new RuntimeException("Close IMap[" + imapName + "]'s MongoDB resource failed, config: " + persistenceMongoDBConfig, throwable);
				})
		);
	}

	public synchronized void store(String key, Object value) {
		if (!checkEnable() || null == mongoDBResource) {
			return;
		}
		if (!(value instanceof Document)) {
			return;
		}
		value = ((Document) value).append("_ts", System.currentTimeMillis() / 1000);
		Document query = sign().append("key", key);
		Document doc = new Document(query).append("value", value);
		ReplaceOptions options = new ReplaceOptions().upsert(true);
		this.mongoDBResource.getMongoCollection().replaceOne(query, doc, options);
	}

	public synchronized void storeAll(Map<String, Object> map) {
		if (!checkEnable() || null == mongoDBResource) {
			return;
		}
		List<WriteModel<Document>> writeModels = new ArrayList<>();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			if (!(entry.getValue() instanceof Document)) {
				continue;
			}
			String key = entry.getKey();
			Object value = entry.getValue();
			value = ((Document) value).append("_ts", System.currentTimeMillis() / 1000);
			Document query = sign().append("key", key);
			Document doc = new Document(query).append("value", value);
			ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
			ReplaceOneModel<Document> replaceOneModel = new ReplaceOneModel<>(query, doc, replaceOptions);
			writeModels.add(replaceOneModel);
			if (writeModels.size() % STORE_ALL_BATCH_SIZE == 0) {
				this.mongoDBResource.getMongoCollection().bulkWrite(writeModels);
				writeModels.clear();
			}
		}
		if (CollectionUtils.isNotEmpty(writeModels)) {
			this.mongoDBResource.getMongoCollection().bulkWrite(writeModels);
			writeModels.clear();
		}
	}

	public synchronized void delete(String key) {
		if (!checkEnable() || null == mongoDBResource) {
			return;
		}
		this.mongoDBResource.getMongoCollection().deleteOne(sign().append("key", key));
	}

	public synchronized void deleteAll(Collection<String> keys) {
		if (!checkEnable() || null == mongoDBResource) {
			return;
		}
		if (CollectionUtils.isNotEmpty(keys)) {
			List<String> tempKeys = new ArrayList<>();
			for (String key : keys) {
				tempKeys.add(key);
				if (tempKeys.size() >= 100) {
					Document deleteClause = sign()
							.append("key", new Document("$in", tempKeys));
					this.mongoDBResource.getMongoCollection().deleteMany(deleteClause);
					tempKeys.clear();
				}
			}
			if (tempKeys.size() > 0) {
				Document deleteClause = sign()
						.append("key", new Document("$in", tempKeys));
				this.mongoDBResource.getMongoCollection().deleteMany(deleteClause);
				tempKeys.clear();
			}
		} else {
			this.mongoDBResource.getMongoCollection().deleteMany(sign());
		}
	}

	public synchronized Document load(String key) {
		if (null != this.mongoDBResource) {
			Document query = sign().append("key", key);
			Document doc = this.mongoDBResource.getMongoCollection().find(query).first();
			if (doc != null) {
				return (Document) doc.get("value");
			}
		}
		return null;
	}

	public synchronized Map<String, Object> loadAll(Collection<String> keys) {
		if (null == this.mongoDBResource || CollectionUtils.isEmpty(keys)) {
			return null;
		}
		Map<String, Object> result = new HashMap<>();
		Collection<String> cache = new HashSet<>();
		for (String key : keys) {
			if (null == key) {
				continue;
			}
			cache.add(key);
			if (cache.size() >= QUERY_BATCH_SIZE) {
				loadAll(cache, result);
				cache.clear();
			}
		}
		if (CollectionUtils.isNotEmpty(cache)) {
			loadAll(cache, result);
			cache.clear();
		}
		return result;
	}

	private void loadAll(Collection<String> keys, Map<String, Object> result) {
		Document query = sign().append("key", new Document("$in", keys));
		for (Document data : this.mongoDBResource.getMongoCollection().find(query)) {
			result.put(data.getString("key"), data);
		}
	}

	public Iterable<String> loadAllKeys() {
		/*FindIterable<Document> findIterable = this.mongoDBResource.getMongoCollection().find(sign());
		MongoDBImapIterable mongoDBImapIterable = new MongoDBImapIterable(findIterable);
		return mongoDBImapIterable;*/
		return null;
	}

	static class MongoDBImapIterable implements Iterable<String> {
		private final FindIterable<Document> mongoIterable;

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

	static class MongoDBImapIterator implements Iterator<String> {
		private final MongoCursor<Document> mongoCursor;

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
