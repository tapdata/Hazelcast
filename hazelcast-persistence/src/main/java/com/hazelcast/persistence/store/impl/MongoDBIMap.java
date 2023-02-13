package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.external.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class MongoDBIMap extends PersistenceMapStore<PersistenceMongoDBConfig, MongoDBResource> {
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
		this.sign = new Document("imap", super.imapName);
	}

	@Override
	public void doDestroy() {
		releaseResource();
	}

	@Override
	public void destroy() {
		this.deleteAll(null);
		releaseResource();
	}

	private void releaseResource() {
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
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			store(entry.getKey(), entry.getValue());
		}
	}

	public synchronized void delete(String key) {
		this.mongoDBResource.getMongoCollection().deleteOne(sign().append("key", key));
	}

	public synchronized void deleteAll(Collection<String> keys) {
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
		Document query = sign().append("key", key);
		Document doc = this.mongoDBResource.getMongoCollection().find(query).first();
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
		return new MongoDBImapIterable(this.mongoDBResource.getMongoCollection().find(new Document("imap", imapName)));
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