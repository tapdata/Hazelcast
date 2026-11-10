package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.ttl.FakeRingBufferStore;
import com.hazelcast.persistence.store.ttl.RingBufferExpireResult;
import com.hazelcast.persistence.store.ttl.TTLConfig;
import com.hazelcast.persistence.store.ttl.TTLMetrics;
import com.hazelcast.persistence.store.ttl.TTLProcessorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.persistence.store.ttl.FakeRingBufferStore.sign;
import static com.hazelcast.persistence.store.ttl.FakeRingBufferStore.value;
import static org.junit.jupiter.api.Assertions.*;

class RingBufferTTLProcessorTest {
	private static final long TTL_SECONDS = TimeUnit.DAYS.toSeconds(3);
	private long expiredTs;
	private long freshTs;
	private FakeRingBufferStore store;
	private RingBufferTTLProcessor processor;

	@BeforeEach
	void setUp() {
		long nowSec = System.currentTimeMillis() / 1000;
		expiredTs = nowSec - TimeUnit.DAYS.toSeconds(10);
		freshTs = nowSec;
		store = new FakeRingBufferStore();
		processor = new RingBufferTTLProcessor();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private TTLMetrics run(int batchSize, long maxDurationMs) {
		TTLProcessorContext ctx = new TTLProcessorContext((PersistenceStorageStore) store, null);
		PersistenceStorageAbstractConfig config = PersistenceMongoDBConfig.create(ConstructType.RINGBUFFER, "rb-test");
		return processor.doTTL(ctx, new TTLConfig(config, TTL_SECONDS), batchSize, 0L, maxDurationMs);
	}

	@Test
	void mixedExpiredAndFresh_stopsAtFirstFresh() {
		for (long i = 0; i < 10; i++) store.put(i, value(expiredTs));
		store.put(10, value(freshTs));
		for (long i = 11; i < 20; i++) store.put(i, value(expiredTs));
		store.put(20, value(freshTs));

		TTLMetrics metrics = run(4, TimeUnit.MINUTES.toMillis(10));

		assertNull(metrics.getError());
		assertEquals(10L, metrics.getDeleteCount());
		assertFalse(metrics.isHasMore());
		for (long i = 0; i < 10; i++) assertFalse(store.data.containsKey(i));
		for (long i = 10; i <= 20; i++) assertTrue(store.data.containsKey(i), "kept " + i);
	}

	@Test
	void fullBatchMeetsFreshInTheMiddle_nextSequenceIsFreshKey() {
		store.put(0, value(expiredTs)).put(1, value(expiredTs)).put(2, value(freshTs)).put(3, value(expiredTs)).put(9, value(expiredTs));
		RingBufferExpireResult result = store.deleteExpired(0, 9, (System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(TTL_SECONDS)) / 1000, 4);
		assertTrue(result.isFinished());
		assertEquals(2L, result.getNextSequence());
		assertEquals(2L, result.getDeletedCount());
	}

	@Test
	void signAtHeadAndMiddle_kept() {
		store.put(0, sign()).put(1, value(expiredTs)).put(2, sign()).put(3, value(expiredTs)).put(4, value(freshTs));
		store.smallestSequence = 0;

		TTLMetrics metrics = run(2, TimeUnit.MINUTES.toMillis(10));

		assertEquals(2L, metrics.getDeleteCount());
		assertTrue(store.data.containsKey(0L));
		assertTrue(store.data.containsKey(2L));
		assertEquals(List.of(1L, 3L), store.deleted);
	}

	@Test
	void recordsWithoutTs_keptAndDoNotStopDeletion() {
		store.put(0, value(expiredTs)).put(1, value(null)).put(2, value("not-a-number")).put(3, value(expiredTs)).put(4, value(freshTs));

		TTLMetrics metrics = run(100, TimeUnit.MINUTES.toMillis(10));

		assertEquals(2L, metrics.getDeleteCount());
		assertTrue(store.data.containsKey(1L));
		assertTrue(store.data.containsKey(2L));
		assertEquals(List.of(0L, 3L), store.deleted);
	}

	@Test
	void allExpired_largestSequenceKept() {
		for (long i = 0; i < 50; i++) store.put(i, value(expiredTs));

		TTLMetrics metrics = run(7, TimeUnit.MINUTES.toMillis(10));

		assertEquals(49L, metrics.getDeleteCount());
		assertFalse(metrics.isHasMore());
		assertEquals(1, store.data.size());
		assertTrue(store.data.containsKey(49L));
	}

	@Test
	void emptyStore_noop() {
		TTLMetrics metrics = run(10, TimeUnit.MINUTES.toMillis(10));
		assertNull(metrics.getError());
		assertEquals(0L, metrics.getDeleteCount());
		assertEquals(0, store.loadCount.get());
	}

	@Test
	void holes_skipped() {
		store.put(0, value(expiredTs)).put(5, value(expiredTs)).put(9, value(expiredTs)).put(10, value(freshTs));

		TTLMetrics metrics = run(3, TimeUnit.MINUTES.toMillis(10));

		assertEquals(3L, metrics.getDeleteCount());
		assertEquals(1, store.data.size());
	}

	@Test
	void defaultSmallestWithoutSign_startsFromStoredSmallest() {
		// Already deleted range [0, 1000) must not be rescanned from 0
		store.smallestSequence = 1000L;
		store.put(1000, value(expiredTs)).put(1001, value(freshTs));

		assertEquals(1000L, store.getSmallestSequenceWithoutSign());
		TTLMetrics metrics = run(10, TimeUnit.MINUTES.toMillis(10));

		assertEquals(1L, metrics.getDeleteCount());
		assertEquals(1, store.loadCount.get(), "only sequence 1000 is loaded, 1001 is the kept largest");
	}

	@Test
	void int32AndInt64Ts_compatible() {
		store.put(0, value((int) expiredTs)).put(1, value(expiredTs)).put(2, value((int) freshTs));

		TTLMetrics metrics = run(10, TimeUnit.MINUTES.toMillis(10));

		assertNull(metrics.getError());
		assertEquals(2L, metrics.getDeleteCount());
	}

	@Test
	void durationLimitZero_hasMoreAndOneBatchOnly() {
		for (long i = 0; i < 100; i++) store.put(i, value(expiredTs));

		TTLMetrics metrics = run(10, 0L);

		assertTrue(metrics.isHasMore());
		assertEquals(10L, metrics.getDeleteCount());
		assertEquals(90, store.data.size());
	}

	@Test
	void interrupted_stopsAndKeepsInterruptFlag() {
		for (long i = 0; i < 100; i++) store.put(i, value(expiredTs));
		Thread.currentThread().interrupt();
		try {
			TTLMetrics metrics = run(10, TimeUnit.MINUTES.toMillis(10));
			assertTrue(metrics.isHasMore());
			assertEquals(0L, metrics.getDeleteCount());
			assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void loadFailure_reportedAsError() {
		FakeRingBufferStore failing = new FakeRingBufferStore() {
			@Override
			public Object load(long sequence) {
				throw new IllegalStateException("boom");
			}
		};
		failing.put(0, value(expiredTs)).put(1, value(expiredTs));
		store = failing;

		TTLMetrics metrics = run(10, TimeUnit.MINUTES.toMillis(10));

		assertNotNull(metrics.getError());
		assertTrue(metrics.getError().getMessage().contains("sequence: 0"));
	}

	@Test
	void nonRingBufferStore_ignored() {
		TTLProcessorContext ctx = new TTLProcessorContext(null, null);
		TTLMetrics metrics = processor.doTTL(ctx, new TTLConfig(PersistenceMongoDBConfig.create(ConstructType.RINGBUFFER, "x"), TTL_SECONDS), 10, 0L, 0L);
		assertEquals(0L, metrics.getDeleteCount());
		assertNull(metrics.getError());
	}

	@Test
	void invalidBatchSize_rejected() {
		assertThrows(IllegalArgumentException.class, () -> store.deleteExpired(0, 10, 0, 0));
	}
}
