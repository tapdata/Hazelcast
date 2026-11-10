package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.ttl.TTLConfig;
import com.hazelcast.persistence.store.ttl.TTLMetrics;
import com.hazelcast.persistence.store.ttl.TTLProcessorContext;
import com.hazelcast.persistence.store.ttl.impl.RingBufferTTLProcessor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Manual benchmark for TAP-13213 (AC1), never runs in normal builds.
 * <pre>
 * TTL_IT_MONGO_URI=... mvn test -Dtest=RingBufferTTLBenchmark -Dttl.benchmark=true \
 *   -Dttl.benchmark.mode=batch|legacy -Dttl.benchmark.records=5000000 -Dttl.benchmark.batchSize=5000 \
 *   -Dttl.benchmark.legacyMaxSeconds=300 -Dttl.benchmark.keep=false
 * </pre>
 * Data: head 1 SIGN, then expired records (_ts 10 days ago, ~1KB), tail 10000 fresh records; TTL = 3 days.
 * Prints one "BENCHMARK|..." line per measurement.
 */
@Tag("manual")
@EnabledIfSystemProperty(named = "ttl.benchmark", matches = "true")
class RingBufferTTLBenchmark {
	private static final long TTL_SECONDS = TimeUnit.DAYS.toSeconds(3);
	private static final int FRESH_TAIL = 10_000;
	private static final int INSERT_BATCH = 10_000;

	@Test
	void benchmark() throws Exception {
		String uri = Optional.ofNullable(System.getProperty(MongoDBRingBufferExpireIT.URI_KEY)).orElse(System.getenv(MongoDBRingBufferExpireIT.URI_KEY));
		if (null == uri || uri.isBlank()) {
			throw new IllegalStateException(MongoDBRingBufferExpireIT.URI_KEY + " is required");
		}
		String mode = System.getProperty("ttl.benchmark.mode", "batch");
		long records = Long.getLong("ttl.benchmark.records", 5_000_000L);
		int batchSize = Integer.getInteger("ttl.benchmark.batchSize", RingBufferTTLProcessor.DEFAULT_BATCH_SIZE);
		long legacyMaxSeconds = Long.getLong("ttl.benchmark.legacyMaxSeconds", 300L);
		boolean keep = Boolean.getBoolean("ttl.benchmark.keep");
		String database = System.getProperty("ttl.benchmark.database", "tap13213_ttl_bench");
		String collectionName = "ExternalStorage_SHARE_CDC_bench_" + mode + "_" + batchSize;

		try (MongoClient client = MongoClients.create(uri)) {
			MongoCollection<Document> collection = client.getDatabase(database).getCollection(collectionName);
			collection.drop();
			createIndexes(collection);
			long expiredTs = System.currentTimeMillis() / 1000 - TimeUnit.DAYS.toSeconds(10);
			long loadStart = System.currentTimeMillis();
			long expired = load(collection, collectionName, records, expiredTs);
			print("load", mode, batchSize, records, "expired=" + expired, "costMs=" + (System.currentTimeMillis() - loadStart));

			org.bson.BsonTimestamp oplogStart = new org.bson.BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0);
			PersistenceMongoDBConfig config = PersistenceMongoDBConfig.create(ConstructType.RINGBUFFER, "bench-" + collectionName)
					.uri(uri).database(database).collection(collectionName);
			config.sequenceMode(PersistenceStorage.SequenceMode.HAZELCAST);
			MongoDBResource resource = new MongoDBResource();
			resource.doInit(config);
			MongoDBRingBuffer store = new MongoDBRingBuffer();
			store.doInit(config, resource);
			try {
				probe("before", store, mode, batchSize, records);
				long start = System.currentTimeMillis();
				long deleted;
				if ("legacy".equals(mode)) {
					deleted = legacy(store, TimeUnit.SECONDS.toMillis(legacyMaxSeconds));
				} else {
					System.setProperty(RingBufferTTLProcessor.BATCH_SIZE_KEY, String.valueOf(batchSize));
					System.setProperty(RingBufferTTLProcessor.MAX_DURATION_PER_OBJECT_MS_KEY, String.valueOf(TimeUnit.DAYS.toMillis(1)));
					deleted = batch(store, config, mode, batchSize, records);
				}
				long cost = System.currentTimeMillis() - start;
				Document oplog = oplogDeletes(client, database + "." + collectionName, oplogStart);
				print("result", mode, batchSize, records, "deleted=" + deleted, "costMs=" + cost,
						String.format("rowsPerSec=%.1f", cost > 0 ? deleted * 1000.0 / cost : 0.0),
						"oplog=" + (null == oplog ? "n/a" : oplog.toJson()),
						"remaining=" + collection.countDocuments());
				probe("after-0m", store, mode, batchSize, records);
				if (!"legacy".equals(mode)) {
					TimeUnit.MINUTES.sleep(1);
					probe("after-1m", store, mode, batchSize, records);
					TimeUnit.MINUTES.sleep(4);
					probe("after-5m", store, mode, batchSize, records);
				}
			} finally {
				store.doDestroy();
				if (!keep) {
					collection.drop();
				}
			}
		}
	}

	private static void createIndexes(MongoCollection<Document> collection) {
		collection.createIndexes(Arrays.asList(
				new IndexModel(Indexes.ascending(MongoDBRingBuffer.SIGN_KEY, "key", "_id")),
				new IndexModel(Indexes.ascending(MongoDBRingBuffer.SIGN_KEY, "value.timestamp", "_id")),
				new IndexModel(Indexes.ascending(MongoDBRingBuffer.SIGN_KEY, "_id", "value.timestamp")),
				new IndexModel(Indexes.ascending(MongoDBRingBuffer.SIGN_KEY, "_id"))
		));
	}

	private static long load(MongoCollection<Document> collection, String sign, long records, long expiredTs) {
		String payload = "p".repeat(900);
		long nowTs = System.currentTimeMillis() / 1000;
		long expired = 0;
		List<Document> buffer = new ArrayList<>(INSERT_BATCH);
		for (long key = 0; key < records; key++) {
			Document value;
			if (key == 0) {
				value = new Document("type", "SIGN").append("_ts", expiredTs);
			} else if (key >= records - FRESH_TAIL) {
				value = new Document("type", "DATA").append("_ts", nowTs).append("timestamp", nowTs * 1000).append("payload", payload);
			} else {
				value = new Document("type", "DATA").append("_ts", expiredTs).append("timestamp", expiredTs * 1000).append("payload", payload);
				expired++;
			}
			buffer.add(new Document(MongoDBRingBuffer.SIGN_KEY, sign).append("key", key).append("value", value));
			if (buffer.size() >= INSERT_BATCH) {
				collection.insertMany(buffer, new InsertManyOptions().ordered(true));
				buffer.clear();
			}
		}
		if (!buffer.isEmpty()) {
			collection.insertMany(buffer, new InsertManyOptions().ordered(true));
		}
		return expired;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private static long batch(MongoDBRingBuffer store, PersistenceMongoDBConfig config, String mode, int batchSize, long records) {
		TTLProcessorContext ctx = new TTLProcessorContext((PersistenceStorageStore) store, null);
		TTLMetrics metrics = new RingBufferTTLProcessor().doTTL(ctx, new TTLConfig(config, TTL_SECONDS));
		if (null != metrics.getError()) {
			throw new IllegalStateException(metrics.getError());
		}
		return metrics.getDeleteCount();
	}

	/**
	 * Previous implementation (release-v5.5.0@4ca2cc0d RingBufferTTLProcessor.doTTL), time boxed:
	 * load(s) + delete(s), where delete calls flushSequence after each deleteOne.
	 */
	private static long legacy(MongoDBRingBuffer store, long maxMs) {
		long ttlMillis = TimeUnit.SECONDS.toMillis(TTL_SECONDS);
		long start = System.currentTimeMillis();
		long deleted = 0;
		long t = store.getLargestSequence();
		long s = store.getSmallestSequenceWithoutSign();
		while (s < t && System.currentTimeMillis() - start < maxMs) {
			try {
				Document document = store.load(s);
				if (null == document || "SIGN".equals(document.get("type"))) {
					continue;
				}
				Object ts = document.get("_ts");
				if (!(ts instanceof Number number)) {
					continue;
				}
				if (System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(number.longValue()) < ttlMillis) {
					break;
				}
				store.delete(s);
				deleted++;
			} finally {
				s++;
			}
		}
		return deleted;
	}

	private static void probe(String phase, MongoDBRingBuffer store, String mode, int batchSize, long records) {
		long t0 = System.nanoTime();
		long smallest = store._getSmallestSequence();
		long t1 = System.nanoTime();
		long smallestWithoutSign = store.getSmallestSequenceWithoutSign();
		long t2 = System.nanoTime();
		store.load(smallestWithoutSign);
		long t3 = System.nanoTime();
		print("probe-" + phase, mode, batchSize, records,
				"smallest=" + smallest, "getSmallestMs=" + ms(t1 - t0),
				"smallestWithoutSign=" + smallestWithoutSign, "getSmallestWithoutSignMs=" + ms(t2 - t1),
				"loadMs=" + ms(t3 - t2));
	}

	private static String ms(long nanos) {
		return String.format("%.2f", nanos / 1_000_000.0);
	}

	/**
	 * Delete oplog entries generated for the benchmark namespace since start: count and total bytes
	 */
	private static Document oplogDeletes(MongoClient client, String namespace, org.bson.BsonTimestamp start) {
		try {
			List<Document> pipeline = Arrays.asList(
					new Document("$match", new Document("ns", namespace).append("op", "d").append("ts", new Document("$gte", start))),
					new Document("$group", new Document("_id", null).append("entries", new Document("$sum", 1))
							.append("bytes", new Document("$sum", new Document("$bsonSize", "$$ROOT")))));
			Document result = client.getDatabase("local").getCollection("oplog.rs").aggregate(pipeline).allowDiskUse(true).first();
			Document stats = client.getDatabase("local").runCommand(new Document("collStats", "oplog.rs"));
			return new Document("deleteEntries", null == result ? 0 : result.get("entries"))
					.append("deleteBytes", null == result ? 0 : result.get("bytes"))
					.append("oplogMaxSize", stats.get("maxSize"));
		} catch (Exception e) {
			return new Document("error", e.getMessage());
		}
	}

	private static void print(String phase, String mode, int batchSize, long records, String... fields) {
		System.out.println("BENCHMARK|" + phase + "|mode=" + mode + "|batchSize=" + batchSize + "|records=" + records + "|" + String.join("|", fields));
	}
}
