package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.ttl.RingBufferExpireResult;
import com.hazelcast.persistence.store.ttl.TTLConfig;
import com.hazelcast.persistence.store.ttl.TTLMetrics;
import com.hazelcast.persistence.store.ttl.TTLProcessorContext;
import com.hazelcast.persistence.store.ttl.impl.RingBufferTTLProcessor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Real MongoDB test for {@link MongoDBRingBuffer#deleteExpired}, focused on the deleteMany filter never deleting
 * SIGN, records without numeric _ts, unexpired records, or the largest sequence.
 * <p>
 * Requires env/system property {@code TTL_IT_MONGO_URI}; skipped otherwise. Each test uses its own database and drops it.
 */
class MongoDBRingBufferExpireIT {
	static final String URI_KEY = "TTL_IT_MONGO_URI";
	private static final long TTL_SECONDS = TimeUnit.DAYS.toSeconds(3);
	private MongoClient client;
	private String database;
	private String collectionName;
	private MongoCollection<Document> collection;
	private String uri;
	private MongoDBRingBuffer store;
	private PersistenceMongoDBConfig config;
	private long expiredTs;
	private long freshTs;
	private final Map<String, String> savedProperties = new HashMap<>();

	@BeforeEach
	void setUp() {
		uri = Optional.ofNullable(System.getProperty(URI_KEY)).orElse(System.getenv(URI_KEY));
		assumeTrue(null != uri && !uri.isBlank(), URI_KEY + " not set, skip real MongoDB test");
		String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 10);
		database = "tap13213_ttl_it_" + suffix;
		collectionName = "ExternalStorage_SHARE_CDC_it_" + suffix;
		client = MongoClients.create(uri);
		collection = client.getDatabase(database).getCollection(collectionName);
		long nowSec = System.currentTimeMillis() / 1000;
		expiredTs = nowSec - TimeUnit.DAYS.toSeconds(10);
		freshTs = nowSec;
	}

	@AfterEach
	void tearDown() {
		savedProperties.forEach((k, v) -> {
			if (null == v) System.clearProperty(k);
			else System.setProperty(k, v);
		});
		savedProperties.clear();
		if (null != store) {
			store.doDestroy();
			store = null;
		}
		if (null != client) {
			client.getDatabase(database).drop();
			client.close();
		}
	}

	private void property(String key, String value) {
		savedProperties.putIfAbsent(key, System.getProperty(key));
		System.setProperty(key, value);
	}

	private Document data(Object ts) {
		Document value = new Document("type", "DATA").append("payload", "x".repeat(64));
		if (null != ts) {
			value.append("_ts", ts);
		}
		return value;
	}

	private Document sign() {
		return new Document("type", "SIGN").append("_ts", expiredTs);
	}

	/**
	 * Insert in key order so that _id (ObjectId) order matches key order, as with real writes
	 */
	private void insert(Object key, Document value) {
		collection.insertOne(new Document(MongoDBRingBuffer.SIGN_KEY, collectionName).append("key", key).append("value", value));
	}

	private MongoDBRingBuffer openStore() {
		config = PersistenceMongoDBConfig.create(ConstructType.RINGBUFFER, "it-" + collectionName)
				.uri(uri).database(database).collection(collectionName);
		// HAZELCAST sequence mode: no background sequence flush threads in the test
		config.sequenceMode(PersistenceStorage.SequenceMode.HAZELCAST);
		MongoDBResource resource = new MongoDBResource();
		resource.doInit(config);
		MongoDBRingBuffer ringBuffer = new MongoDBRingBuffer();
		ringBuffer.doInit(config, resource);
		store = ringBuffer;
		return ringBuffer;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private TTLMetrics runProcessor(int batchSize, long maxDurationMs) {
		property(RingBufferTTLProcessor.BATCH_SIZE_KEY, String.valueOf(batchSize));
		property(RingBufferTTLProcessor.MAX_DURATION_PER_OBJECT_MS_KEY, String.valueOf(maxDurationMs));
		property(RingBufferTTLProcessor.BATCH_INTERVAL_MS_KEY, "0");
		MongoDBRingBuffer ringBuffer = openStore();
		TTLProcessorContext ctx = new TTLProcessorContext((PersistenceStorageStore) ringBuffer, null);
		TTLMetrics metrics = new RingBufferTTLProcessor().doTTL(ctx, new TTLConfig(config, TTL_SECONDS));
		assertNull(metrics.getError(), () -> String.valueOf(metrics.getError()));
		return metrics;
	}

	private Set<Long> remainingKeys() {
		return collection.find().into(new ArrayList<>()).stream()
				.map(d -> ((Number) d.get("key")).longValue()).collect(Collectors.toCollection(TreeSet::new));
	}

	@Test
	void signNoTsStringTsAndUnexpired_neverDeleted() {
		insert(0L, sign());
		insert(1L, data(expiredTs));
		insert(2L, data(null));
		insert(3L, data(String.valueOf(expiredTs)));
		insert(4L, data(expiredTs));
		insert(5L, data(expiredTs));
		insert(6L, data(freshTs));
		insert(7L, data(expiredTs));
		insert(8L, data(expiredTs));

		TTLMetrics metrics = runProcessor(3, TimeUnit.MINUTES.toMillis(10));

		assertEquals(3L, metrics.getDeleteCount());
		assertFalse(metrics.isHasMore());
		assertEquals(new TreeSet<>(List.of(0L, 2L, 3L, 6L, 7L, 8L)), remainingKeys());
	}

	@Test
	void nonMonotonicTs_stopsAtUnexpired_noDeleteBeyond() {
		insert(0L, data(expiredTs));
		insert(1L, data(expiredTs));
		insert(2L, data(freshTs));
		insert(3L, data(expiredTs));
		insert(4L, data(expiredTs));
		MongoDBRingBuffer ringBuffer = openStore();
		long cutoff = System.currentTimeMillis() / 1000 - TTL_SECONDS;

		RingBufferExpireResult result = ringBuffer.deleteExpired(0L, 4L, cutoff, 100);

		assertTrue(result.isFinished());
		assertEquals(2L, result.getNextSequence());
		assertEquals(2L, result.getDeletedCount());
		assertEquals(new TreeSet<>(List.of(2L, 3L, 4L)), remainingKeys());
	}

	@Test
	void deleteFilter_protectsNonCandidatesInsideRange() {
		// Candidate range [0, 4] contains SIGN, no _ts and string _ts records, only numeric expired ones are removed
		insert(0L, data(expiredTs));
		insert(1L, sign());
		insert(2L, data(null));
		insert(3L, data("abc"));
		insert(4L, data(expiredTs));
		insert(5L, data(expiredTs));
		MongoDBRingBuffer ringBuffer = openStore();
		long cutoff = System.currentTimeMillis() / 1000 - TTL_SECONDS;

		RingBufferExpireResult result = ringBuffer.deleteExpired(0L, 5L, cutoff, 100);

		assertTrue(result.isFinished());
		assertEquals(2L, result.getDeletedCount());
		assertEquals(new TreeSet<>(List.of(1L, 2L, 3L, 5L)), remainingKeys());
	}

	@Test
	void allExpired_onlyLargestKept_largestSequenceUnchanged() {
		for (long i = 0; i < 100; i++) {
			insert(i, data(expiredTs));
		}

		TTLMetrics metrics = runProcessor(7, TimeUnit.MINUTES.toMillis(10));

		assertEquals(99L, metrics.getDeleteCount());
		assertEquals(Set.of(99L), remainingKeys());
		assertEquals(99L, store._getLargestSequence());
	}

	@Test
	void holesAcrossMultipleBatches() {
		for (long i = 0; i < 100; i++) {
			insert(i * 10, data(expiredTs));
		}
		insert(1000L, data(freshTs));

		TTLMetrics metrics = runProcessor(7, TimeUnit.MINUTES.toMillis(10));

		assertEquals(100L, metrics.getDeleteCount());
		assertEquals(Set.of(1000L), remainingKeys());
	}

	@Test
	void int32KeyAndTs_deleted() {
		for (int i = 0; i < 5; i++) {
			insert(i, data((int) expiredTs));
		}
		insert(5L, data(freshTs));

		TTLMetrics metrics = runProcessor(2, TimeUnit.MINUTES.toMillis(10));

		assertEquals(5L, metrics.getDeleteCount());
		assertEquals(Set.of(5L), remainingKeys());
	}

	@Test
	void durationLimitZero_oneBatchAndHasMore() {
		for (long i = 0; i < 51; i++) {
			insert(i, data(expiredTs));
		}

		TTLMetrics metrics = runProcessor(10, 0L);

		assertTrue(metrics.isHasMore());
		assertEquals(10L, metrics.getDeleteCount());
		assertEquals(41, remainingKeys().size());
	}

	@Test
	void smallestWithoutSign_skipsHeadSign() {
		insert(0L, sign());
		insert(1L, data(expiredTs));
		insert(2L, data(freshTs));
		MongoDBRingBuffer ringBuffer = openStore();

		assertEquals(1L, ringBuffer.getSmallestSequenceWithoutSign());
	}
}
