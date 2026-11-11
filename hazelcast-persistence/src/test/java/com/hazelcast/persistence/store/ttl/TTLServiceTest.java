package com.hazelcast.persistence.store.ttl;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import com.hazelcast.persistence.config.PersistenceInMemConfig;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class TTLServiceTest {
	private static final long LONG_INTERVAL = TimeUnit.MINUTES.toMillis(30);
	private final List<String> warnings = new CopyOnWriteArrayList<>();
	private final Map<String, AtomicInteger> created = new ConcurrentHashMap<>();
	private final Map<String, FakeRingBufferStore> stores = new ConcurrentHashMap<>();
	private final Set<String> nullStores = ConcurrentHashMap.newKeySet();
	private final Map<String, Integer> processed = new ConcurrentHashMap<>();
	private final Set<String> processorThreads = ConcurrentHashMap.newKeySet();
	private TTLService service;

	private Logger logger() {
		return (Logger) Proxy.newProxyInstance(Logger.class.getClassLoader(), new Class[]{Logger.class}, (proxy, method, args) -> {
			if ("warn".equals(method.getName()) && null != args && args.length > 0) {
				StringBuilder sb = new StringBuilder(String.valueOf(args[0]));
				for (int i = 1; i < args.length; i++) {
					sb.append(" | ").append(args[i] instanceof Object[] arr ? Arrays.toString(arr) : args[i]);
				}
				warnings.add(sb.toString());
			}
			Class<?> type = method.getReturnType();
			if (type == boolean.class) return false;
			if (type.isPrimitive() && type != void.class) return 0;
			return null;
		});
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private Function<PersistenceStorageAbstractConfig, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>> createStore() {
		return config -> {
			created.computeIfAbsent(config.getName(), k -> new AtomicInteger()).incrementAndGet();
			if (nullStores.contains(config.getName())) {
				return null;
			}
			FakeRingBufferStore store = new FakeRingBufferStore();
			stores.put(config.getName(), store);
			return (PersistenceStorageStore) store;
		};
	}

	private TTLService newService(TTLProcessor processor, long interval, long backlogInterval, int threads) {
		return new TTLService(createStore(), logger(), type -> processor, interval, backlogInterval, threads, false);
	}

	private TTLProcessor recordingProcessor(Function<String, TTLMetrics> behavior) {
		return (ctx, cfg) -> {
			String name = cfg.getPersistenceStorageAbstractConfig().getName();
			processed.merge(name, 1, Integer::sum);
			processorThreads.add(String.valueOf(Thread.currentThread().getId()));
			return behavior.apply(name);
		};
	}

	private static TTLMetrics metrics(String name, boolean hasMore) {
		TTLMetrics metrics = new TTLMetrics(name);
		metrics.setDeleteCount(5L);
		metrics.setHasMore(hasMore);
		return metrics;
	}

	private static PersistenceStorageAbstractConfig mongo(String name) {
		return PersistenceMongoDBConfig.create(ConstructType.RINGBUFFER, name);
	}

	private static boolean await(java.util.function.BooleanSupplier condition, long timeoutMs) throws InterruptedException {
		long deadline = System.currentTimeMillis() + timeoutMs;
		while (System.currentTimeMillis() < deadline) {
			if (condition.getAsBoolean()) return true;
			Thread.sleep(10);
		}
		return condition.getAsBoolean();
	}

	@BeforeEach
	void setUp() {
		TTLService.clearConfigs();
	}

	@AfterEach
	void tearDown() {
		if (null != service) {
			service.stop();
		}
		TTLService.clearConfigs();
	}

	@Test
	void memHttpAndNullStore_skippedWithoutStopping_otherObjectsProcessed() {
		service = newService(recordingProcessor(n -> metrics(n, false)), LONG_INTERVAL, LONG_INTERVAL, 1);
		service.registerTTL(PersistenceInMemConfig.create(ConstructType.RINGBUFFER, "mem-1"), 60, null);
		service.registerTTL(new PersistenceInMemConfig(ConstructType.RINGBUFFER, StorageMode.HTTP_TM, "http-1"), 60, null);
		for (String name : List.of("null-1", "null-2")) {
			nullStores.add(name);
			service.registerTTL(mongo(name), 60, null);
		}
		for (int i = 0; i < 5; i++) {
			service.registerTTL(mongo("rb-" + i), 60, null);
		}

		TTLService.RoundResult result = service.doTTL();

		// Mem/HTTP_TM never create a store (no leak); null store skipped
		assertNull(created.get("mem-1"));
		assertNull(created.get("http-1"));
		assertEquals(1, created.get("null-1").get());
		assertFalse(processed.containsKey("null-1"));
		for (int i = 0; i < 5; i++) {
			assertEquals(1, processed.get("rb-" + i), "rb-" + i + " processed in the same partition");
			assertEquals(1, stores.get("rb-" + i).destroyCount.get(), "store destroyed");
		}
		assertEquals(5, result.objects);
		assertEquals(25L, result.deleted);
		assertFalse(result.hasMore);
	}

	@Test
	void emptyConfigs_roundDoesNotThrow_andScheduleKeepsRunning() throws InterruptedException {
		service = newService(recordingProcessor(n -> metrics(n, false)), 30, 30, 0);
		AtomicInteger rounds = new AtomicInteger();
		service.setRoundTask(() -> {
			rounds.incrementAndGet();
			return service.doTTL();
		});
		assertDoesNotThrow(() -> service.doTTL());

		service.start();

		assertTrue(await(() -> rounds.get() >= 3, 3000), "rounds: " + rounds.get());
		assertTrue(warnings.isEmpty(), warnings.toString());
	}

	@Test
	void processorThrowsError_objectWarned_otherObjectsProcessed_nextRoundRuns() throws InterruptedException {
		service = newService(recordingProcessor(n -> {
			if ("bad".equals(n)) throw new Error("processor boom");
			return metrics(n, false);
		}), 30, 30, 1);
		service.registerTTL(mongo("bad"), 60, null);
		service.registerTTL(mongo("good-1"), 60, null);
		service.registerTTL(mongo("good-2"), 60, null);
		AtomicInteger rounds = new AtomicInteger();
		service.setRoundTask(() -> {
			rounds.incrementAndGet();
			return service.doTTL();
		});

		service.start();

		assertTrue(await(() -> rounds.get() >= 2 && processed.getOrDefault("good-2", 0) >= 2, 3000), "rounds: " + rounds.get());
		assertTrue(processed.getOrDefault("good-1", 0) >= 2);
		assertTrue(warnings.stream().anyMatch(w -> w.contains("Do ttl failed") && w.contains("bad") && w.contains("processor boom")), warnings.toString());
		assertTrue(stores.get("bad").destroyCount.get() >= 1, "store destroyed even when processor throws");
	}

	@Test
	void roundThrows_warnedAndRescheduled() throws InterruptedException {
		service = newService(recordingProcessor(n -> metrics(n, false)), 30, 30, 0);
		AtomicInteger rounds = new AtomicInteger();
		service.setRoundTask(() -> {
			rounds.incrementAndGet();
			throw new Error("round boom");
		});

		service.start();

		assertTrue(await(() -> rounds.get() >= 3, 3000), "rounds: " + rounds.get());
		assertTrue(warnings.stream().anyMatch(w -> w.contains("Do ttl round failed") && w.contains("round boom")), warnings.toString());
	}

	@Test
	void hasMore_usesBacklogInterval() throws InterruptedException {
		service = newService(recordingProcessor(n -> metrics(n, false)), LONG_INTERVAL, 30, 0);
		AtomicInteger rounds = new AtomicInteger();
		service.setRoundTask(() -> {
			rounds.incrementAndGet();
			return new TTLService.RoundResult(1, 1L, true, 1L);
		});

		service.start();

		assertTrue(await(() -> rounds.get() >= 3, 3000), "rounds: " + rounds.get());
	}

	@Test
	void noBacklog_usesNormalInterval() throws InterruptedException {
		service = newService(recordingProcessor(n -> metrics(n, false)), LONG_INTERVAL, 30, 0);
		AtomicInteger rounds = new AtomicInteger();
		service.setRoundTask(() -> {
			rounds.incrementAndGet();
			return new TTLService.RoundResult(1, 1L, false, 1L);
		});

		service.start();

		assertTrue(await(() -> rounds.get() >= 1, 3000));
		Thread.sleep(300);
		assertEquals(1, rounds.get());
	}

	@Test
	void backlogThreeRounds_warns_thenResetsWhenDrained() {
		Map<String, Boolean> backlog = new ConcurrentHashMap<>(Map.of("big", true, "small", false));
		service = newService(recordingProcessor(n -> metrics(n, backlog.get(n))), LONG_INTERVAL, LONG_INTERVAL, 0);
		service.registerTTL(mongo("big"), 60, null);
		service.registerTTL(mongo("small"), 60, null);

		assertTrue(service.doTTL().hasMore);
		assertTrue(service.doTTL().hasMore);
		assertEquals(2, service.backlogRounds("big"));
		assertTrue(warnings.stream().noneMatch(w -> w.contains("TTL backlog is not draining")));

		assertTrue(service.doTTL().hasMore);
		assertEquals(3, service.backlogRounds("big"));
		assertEquals(0, service.backlogRounds("small"));
		assertTrue(warnings.stream().anyMatch(w -> w.contains("TTL backlog is not draining") && w.contains("big") && w.contains("3")), warnings.toString());

		backlog.put("big", false);
		assertFalse(service.doTTL().hasMore);
		assertEquals(0, service.backlogRounds("big"));
	}

	@Test
	void removeTTL_clearsBacklogAndToleratesNull() {
		service = newService(recordingProcessor(n -> metrics(n, true)), LONG_INTERVAL, LONG_INTERVAL, 0);
		PersistenceStorageAbstractConfig config = mongo("big");
		service.registerTTL(config, 60, null);
		service.doTTL();
		assertEquals(1, service.backlogRounds("big"));

		assertDoesNotThrow(() -> service.removeTTL(null));
		service.removeTTL(config);

		assertEquals(0, service.backlogRounds("big"));
		assertEquals(0, service.doTTL().objects);
	}

	@Test
	void partition_neverExceedsThreadNumber() {
		for (int n = 1; n <= 40; n++) {
			List<String> names = new ArrayList<>();
			for (int i = 0; i < n; i++) names.add("n" + i);
			List<List<String>> partition = TTLService.partition(names, 4);
			assertTrue(partition.size() <= 4, "n=" + n + " partitions=" + partition.size());
			assertEquals(n, partition.stream().mapToInt(List::size).sum());
		}
		assertTrue(TTLService.partition(Collections.emptyList(), 4).isEmpty());
	}

	@Test
	void sevenObjects_concurrentThreadsCapped() {
		service = newService(recordingProcessor(n -> {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return metrics(n, false);
		}), LONG_INTERVAL, LONG_INTERVAL, 4);
		for (int i = 0; i < 7; i++) {
			service.registerTTL(mongo("o-" + i), 60, null);
		}

		assertEquals(7, service.doTTL().objects);
		assertTrue(processorThreads.size() <= 4, processorThreads.toString());
	}

	@Test
	void batchRetries_warnsWithBatchSize() {
		service = newService(recordingProcessor(n -> {
			TTLMetrics m = metrics(n, false);
			m.setBatchRetries(2);
			m.setBatchSize(1250);
			m.setLastBatchError("Timeout while receiving message");
			return m;
		}), LONG_INTERVAL, LONG_INTERVAL, 0);
		service.registerTTL(mongo("slow"), 60, null);

		service.doTTL();

		assertTrue(warnings.stream().anyMatch(w -> w.contains("retried with a smaller batch") && w.contains("slow")
				&& w.contains("1250") && w.contains("Timeout")), warnings.toString());
	}
}
