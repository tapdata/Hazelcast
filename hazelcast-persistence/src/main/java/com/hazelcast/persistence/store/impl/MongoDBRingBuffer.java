package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.constants.ShareCDCConstant;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class MongoDBRingBuffer extends PersistenceRingBufferStore<PersistenceMongoDBConfig, MongoDBResource> implements MemoryFetcher {
	public static final int DEFAULT_FIND_LIMIT = 100;
	public static final long FLUSH_SMALLEST_PERIOD_MS = TimeUnit.SECONDS.toMillis(10L);
	public static final long FLUSH_LARGEST_PERIOD_MS = TimeUnit.SECONDS.toMillis(1L);
	public static final String TAG = MongoDBRingBuffer.class.getSimpleName();
	public static final String LOG_PREFIX = "[" + TAG + "]";
	public static final String SIGN_KEY = "ringBuffer";
	public static final String VALUE_KEY = "value";
	public static final String LOAD_CACHE_LIMIT_KEY = "LOAD_CACHE_LIMIT";
	private final AtomicLong largestSequence = new AtomicLong(-1L);
	private final AtomicLong smallestSequence = new AtomicLong(0L);
	private Document sign;
	private MongoDBResource mongoDBResource;
	private PersistenceMongoDBConfig persistenceMongoDBConfig;
	private Map<String, Document> cacheMap;
	private ScheduledThreadPoolExecutor flushSeqScheduler;
	private int flushLargestSleepTime = 0;
	private int flushSmallestSleepTime = 0;
	private int loadCacheLimit;

	private Document sign() {
		return new Document(sign);
	}

	@Override
	public void doInit(PersistenceMongoDBConfig persistenceMongoDBConfig, MongoDBResource mongoDBResource) {
		super.doInit(persistenceMongoDBConfig, mongoDBResource);
		this.mongoDBResource = mongoDBResource;
		this.persistenceMongoDBConfig = persistenceMongoDBConfig;
		createIndex();
		sign = new Document(SIGN_KEY, this.mongoDBResource.getMongoCollection().getNamespace().getCollectionName());
		flushSequence();
		loadCacheLimit = CommonUtils.getPropertyInt(LOAD_CACHE_LIMIT_KEY, DEFAULT_FIND_LIMIT);
		this.cacheMap = new HashMap<>(loadCacheLimit);
		int shareCDCDelayMode = io.tapdata.pdk.core.utils.CommonUtils.shareCDCDelayMode();

		if (persistenceMongoDBConfig.getSequenceMode() == PersistenceStorage.SequenceMode.STORE) {
			this.flushSeqScheduler = new ScheduledThreadPoolExecutor(2);
			this.flushSeqScheduler.scheduleWithFixedDelay(() -> {
				Thread.currentThread().setName("Flush-MongoDB-Ringbuffer-Largest-Sequence-Scheduler-" + ringBufferName);
				long lastSeq = this.largestSequence.get();
				CommonUtils.ignoreAnyError(() -> this.largestSequence.set(_getLargestSequence()));
				if (shareCDCDelayMode == ShareCDCConstant.DELAY_MODE_DEFAULT) {
					flushSeqSleep(lastSeq, 1);
				}
			}, 0, shareCDCDelayMode == ShareCDCConstant.DELAY_MODE_LOW ? 100L : FLUSH_LARGEST_PERIOD_MS, TimeUnit.MILLISECONDS);
			this.flushSeqScheduler.scheduleWithFixedDelay(() -> {
				Thread.currentThread().setName("Flush-MongoDB-Ringbuffer-Smallest-Sequence-Scheduler-" + ringBufferName);
				long lastSeq = this.smallestSequence.get();
				CommonUtils.ignoreAnyError(() -> this.smallestSequence.set(_getSmallestSequence()));
				flushSeqSleep(lastSeq, 2);
			}, 0, FLUSH_SMALLEST_PERIOD_MS, TimeUnit.MILLISECONDS);
		}
		if (null != persistenceMongoDBConfig.getLogger() && persistenceMongoDBConfig.getLogger().isDebugEnabled()) {
			persistenceMongoDBConfig.getLogger().info(LOG_PREFIX + " Init finished, ringbuffer name: '{}', name space: '{}', head seq: {}, tail seq: {}",
					persistenceMongoDBConfig.getName(), mongoDBResource.getMongoCollection().getNamespace().getFullName(),
					this.smallestSequence.get(), this.largestSequence.get());
		}
		CommonUtils.ignoreAnyError(() -> PDKIntegration.registerMemoryFetcher(genMemoryKey(), this));
	}

	@Override
	public void lightInit() {
		super.lightInit();
		flushSequence();
		persistenceMongoDBConfig.getLogger().info(LOG_PREFIX + " Light init finished, ringbuffer name: '{}', name space: '{}', head seq: {}, tail seq: {}",
				persistenceMongoDBConfig.getName(), mongoDBResource.getMongoCollection().getNamespace().getFullName(),
				this.smallestSequence.get(), this.largestSequence.get());
	}

	@Override
	public void reInitResource(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
		if (persistenceStorageAbstractConfig instanceof PersistenceMongoDBConfig newConfig) {
			mongoDBResource.reInitDbAndCollection(newConfig);
			this.persistenceMongoDBConfig = newConfig;
			this.sign = new Document(SIGN_KEY, this.mongoDBResource.getMongoCollection().getNamespace().getCollectionName());
		}
	}

	private void flushSeqSleep(long lastSeq, int type) {
		long currentSeq;
		if (type == 1) {
			currentSeq = this.largestSequence.get();
		} else {
			currentSeq = this.smallestSequence.get();
		}
		long sleepMS = calcSleepTime(unused -> currentSeq == lastSeq, type == 1 ? flushLargestSleepTime : flushSmallestSleepTime);
		if (sleepMS > 0) {
			try {
				Thread.sleep(sleepMS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			if (type == 1) {
				flushLargestSleepTime++;
			} else {
				flushSmallestSleepTime++;
			}
		} else {
			if (type == 1) {
				flushLargestSleepTime = 0;
			} else {
				flushSmallestSleepTime = 0;
			}
		}
	}

	private long calcSleepTime(Predicate<Void> needSleep, int sleepTime) {
		if (null == needSleep) {
			return 0L;
		}
		if (needSleep.test(null)) {
			int factor = Math.min(sleepTime, 10);
			factor = Math.max(1, factor);
			return factor * 500L;
		} else {
			return 0L;
		}
	}

	private String genMemoryKey() {
		return String.format("%s-%s", TAG, ringBufferName);
	}

	private void createIndex() {
		if (mongoDBResource == null) {
			return;
		}
		IndexOptions indexOptions = new IndexOptions().background(true);
		CreateIndexOptions createIndexOptions = new CreateIndexOptions();
		if (MongodbUtil.isIndexCommitQuorumSupported(mongoDBResource.getMongoDatabase())) {
			createIndexOptions.commitQuorum(CreateIndexCommitQuorum.MAJORITY);
		}
		MongoCollection<Document> mongoCollection = mongoDBResource.getMongoCollection();
		List<IndexModel> indexModels = Arrays.asList(
				new IndexModel(Indexes.ascending(SIGN_KEY, "key", "_id"), indexOptions),
				new IndexModel(Indexes.ascending(SIGN_KEY, "value.timestamp", "_id"), indexOptions),
				new IndexModel(Indexes.ascending(SIGN_KEY, "_id"), indexOptions)
		);
		mongoCollection.createIndexes(indexModels, createIndexOptions);
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
		Optional.ofNullable(this.flushSeqScheduler).ifPresent(f -> CommonUtils.ignoreAnyError(f::shutdownNow));
		Optional.of(cacheMap).ifPresent(Map::clear);
		CommonUtils.ignoreAnyError(() -> PDKIntegration.unregisterMemoryFetcher(genMemoryKey()));
	}

	@Override
	public void store(long sequence, Object value) {
		if (!(value instanceof Document document)) {
			return;
		}
		if (!checkEnable() || mongoDBResource == null) {
			return;
		}
		Document doc = getInsertDocument(largestSequence.incrementAndGet(), document);
		this.mongoDBResource.getMongoCollection().insertOne(doc);
	}

	@Override
	public void storeAll(long l, Object[] values) {
		if (!checkEnable() || mongoDBResource == null) {
			return;
		}
		List<WriteModel<Document>> models = new ArrayList<>();
		for (Object value : values) {
			if (!(value instanceof Document document)) {
				continue;
			}
			Document insertDocument = getInsertDocument(largestSequence.incrementAndGet(), document);
			models.add(new InsertOneModel<>(insertDocument));
		}
		this.mongoDBResource.getMongoCollection().bulkWrite(models, new BulkWriteOptions().ordered(true));
	}

	private Document getInsertDocument(long sequence, Document value) {
		return new Document(sign()).append("key", sequence).append(VALUE_KEY, value.append("_ts", System.currentTimeMillis() / 1000));
	}

	@Override
	public Document load(long sequence) {
		if (!checkEnable() || mongoDBResource == null) {
			return null;
		}
		String sequenceStr = String.valueOf(sequence);
		if (!cacheMap.containsKey(sequenceStr)) {
			cacheMap.clear();
			Document query = sign().append("key", new Document("$gte", sequence));
			try (
					MongoCursor<Document> iterator = this.mongoDBResource.getMongoCollection().find(query)
							.sort(Sorts.ascending("key"))
							.limit(loadCacheLimit).iterator()
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
		if (document.containsKey(VALUE_KEY)) {
			return (Document) document.get(VALUE_KEY);
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
		Document doc = this.mongoDBResource.getMongoCollection().find(query).sort(descending("_id")).first();
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
		Document doc = this.mongoDBResource.getMongoCollection().find(query).sort(ascending("_id")).first();
		if (doc == null) {
			return 0;
		}
		return doc.getLong("key");
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = new DataMap();
		try {
			dataMap.kv("smallest sequence", this.smallestSequence.get());
			dataMap.kv("largest sequence", this.largestSequence.get());
		} catch (Exception e) {
			dataMap.kv("error", e.getMessage() + "; Stack: " + ExceptionUtils.getStackTrace(e));
		}
		return dataMap;
	}

	@Override
	public long getSmallestSequenceWithoutSign() {
		Document query = sign().append("value.type", new Document("$ne", "SIGN"));
		Document doc = this.mongoDBResource.getMongoCollection().find(query).sort(ascending("_id")).first();
		if (doc == null) {
			return 0;
		}
		return doc.getLong("key");
	}
}