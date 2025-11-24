package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.util.*;
import java.util.function.Consumer;

public class MongoDBIMap extends PersistenceMapStore<PersistenceMongoDBConfig, MongoDBResource> {
	protected static final int STORE_ALL_BATCH_SIZE = 1000;
	protected static final int QUERY_BATCH_SIZE = 20;
	public static final String VALUE_KEY = "value";
	protected Document sign;
	private MongoDBResource mongoDBResource;
	private PersistenceMongoDBConfig persistenceMongoDBConfig;

	private Document sign() {
		return new Document(sign);
	}

	public MongoDBIMap() {
		// do nothing
	}

	@Override
	public void doInit(PersistenceMongoDBConfig persistenceMongoDBConfig, MongoDBResource mongoDBResource) {
		super.doInit(persistenceMongoDBConfig, mongoDBResource);
		this.mongoDBResource = mongoDBResource;
		this.persistenceMongoDBConfig = persistenceMongoDBConfig;
		createIndex();
		this.sign = new Document("imap", mongoDBResource.getMongoCollection().getNamespace().getCollectionName());
	}

	private void createIndex() {
		IndexOptions indexOptions = new IndexOptions().background(true);
		CreateIndexOptions createIndexOptions = new CreateIndexOptions();
		if (MongodbUtil.isIndexCommitQuorumSupported(mongoDBResource.getMongoDatabase())) {
			createIndexOptions.commitQuorum(CreateIndexCommitQuorum.MAJORITY);
		}
		MongoCollection<Document> mongoCollection = mongoDBResource.getMongoCollection();
		List<IndexModel> indexModels = Arrays.asList(
				new IndexModel(Indexes.ascending("key", "imap"), indexOptions),
				new IndexModel(Indexes.ascending("key", "ts"), indexOptions)
		);
		mongoCollection.createIndexes(indexModels, createIndexOptions);
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
		if (this.persistenceMongoDBConfig.isExclusiveCollection() && null != mongoDBResource) {
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
		Document doc = new Document(query).append(VALUE_KEY, value);
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
			Document doc = new Document(query).append(VALUE_KEY, value);
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
				return (Document) doc.get(VALUE_KEY);
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
			if (null == data) continue;
			if (!data.containsKey(VALUE_KEY)) continue;
			if (!(data.get(VALUE_KEY) instanceof Map)) continue;
			result.put(data.getString("key"), data.get(VALUE_KEY));
		}
	}

	public Iterable<String> loadAllKeys() {
		// do not support this function
		return null;
	}

	@Override
	public Iterable<Document> iterator() {
		FindIterable<Document> findIterable = this.mongoDBResource.getMongoCollection().find(sign());
		return new MongoDBImapIterable(findIterable);
	}

	static class MongoDBImapIterable implements Iterable<Document> {
		private final FindIterable<Document> mongoIterable;

		public MongoDBImapIterable(FindIterable<Document> mongoIterable) {
			if (null == mongoIterable) {
				throw new IllegalArgumentException("MongoIterable is null");
			}
			this.mongoIterable = mongoIterable;
		}

		@Override
		public void forEach(Consumer<? super Document> action) {
			mongoIterable.forEach((Consumer<Document>) action::accept);
		}

		@Override
		public Iterator<Document> iterator() {
			return new MongoDBImapIterator(mongoIterable.iterator());
		}
	}

	static class MongoDBImapIterator implements Iterator<Document> {
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
		public Document next() {
			return mongoCursor.next();
		}
	}

	@Override
	public boolean isEmpty() {
		try (MongoCursor<Document> mongoCursor = mongoDBResource.getMongoCollection().find().limit(1).iterator()) {
			return !mongoCursor.hasNext();
		}
	}

	@Override
	public synchronized Map<String, Object> getStatistics() {
		if (!checkEnable() || null == mongoDBResource) {
			return null;
		}
		Document result = this.mongoDBResource.getMongoDatabase().runCommand(new Document("collStats", persistenceMongoDBConfig.getCollection()));
		Map<String, Object> statistics = new HashMap<>();
		statistics.put("count", result.getInteger("count"));
		statistics.put("size", result.getInteger("storageSize"));
		statistics.put("uri",persistenceMongoDBConfig.uriInfo());
		statistics.put("mode",persistenceMongoDBConfig.getStorageMode().name());
		return statistics;
	}
}
