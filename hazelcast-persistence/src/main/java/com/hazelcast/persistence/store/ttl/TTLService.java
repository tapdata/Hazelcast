package com.hazelcast.persistence.store.ttl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.PDKIntegration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 15:59
 **/
public class TTLService implements MemoryFetcher {
	private static final Map<String, TTLConfig> configs = new ConcurrentHashMap<>();
	public static final int DEFAULT_TTL_PROCESS_THREAD_NUMBER = 4;
	private final static Map<String, TTLProcessor> TTL_PROCESSOR_MAP = new HashMap<>();
	public static final long TTL_INTERVAL_MS = TimeUnit.MINUTES.toMillis(60L);
	public static final String TTL_BACKLOG_INTERVAL_MS_KEY = "TTL_BACKLOG_INTERVAL_MS";
	public static final long DEFAULT_TTL_BACKLOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1L);
	static final int BACKLOG_WARN_ROUNDS = 3;
	private final AtomicBoolean isRunning = new AtomicBoolean();
	private final Function<PersistenceStorageAbstractConfig, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>> createStoreFunc;
	private final Logger logger;
	private final Function<ConstructType, TTLProcessor> processorLookup;
	private final long intervalMs;
	private final long backlogIntervalMs;
	private final int threadNumber;
	/**
	 * Consecutive rounds in which an object still had backlog after the round
	 */
	private final Map<String, AtomicInteger> backlogRounds = new ConcurrentHashMap<>();
	private volatile Callable<RoundResult> roundTask = this::doTTL;
	private volatile ScheduledExecutorService scheduler;

	public TTLService(
			Function<PersistenceStorageAbstractConfig, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>> createStoreFunc,
			Logger logger
	) {
		this(createStoreFunc, logger, null, TTL_INTERVAL_MS,
				CommonUtils.getPropertyLong(TTL_BACKLOG_INTERVAL_MS_KEY, DEFAULT_TTL_BACKLOG_INTERVAL_MS), 0, true);
	}

	/**
	 * Package level constructor for tests: processor lookup, intervals, thread number and memory fetcher registration can be replaced.
	 *
	 * @param threadNumber max ttl threads per round, {@code <= 0} means {@code min(CPU, DEFAULT_TTL_PROCESS_THREAD_NUMBER)}
	 */
	TTLService(
			Function<PersistenceStorageAbstractConfig, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>> createStoreFunc,
			Logger logger,
			Function<ConstructType, TTLProcessor> processorLookup,
			long intervalMs,
			long backlogIntervalMs,
			int threadNumber,
			boolean registerMemoryFetcher
	) {
		if (null == createStoreFunc) {
			throw new IllegalArgumentException("Create store function cannot be null");
		}
		this.createStoreFunc = createStoreFunc;
		this.logger = logger;
		for (ConstructType value : ConstructType.values()) {
			Supplier<TTLProcessor> ttlProcessor = value.getTtlProcessor();
			if (null != ttlProcessor) {
				TTL_PROCESSOR_MAP.put(value.name(), ttlProcessor.get());
			}
		}
		this.processorLookup = null != processorLookup ? processorLookup : type -> null == type ? null : TTL_PROCESSOR_MAP.get(type.name());
		this.intervalMs = intervalMs > 0 ? intervalMs : TTL_INTERVAL_MS;
		this.backlogIntervalMs = backlogIntervalMs > 0 ? backlogIntervalMs : DEFAULT_TTL_BACKLOG_INTERVAL_MS;
		this.threadNumber = threadNumber > 0 ? threadNumber : Math.min(Runtime.getRuntime().availableProcessors(), DEFAULT_TTL_PROCESS_THREAD_NUMBER);
		if (registerMemoryFetcher) {
			PDKIntegration.registerMemoryFetcher(TTLService.class.getSimpleName(), this);
		}
	}

	public void registerTTL(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig, long ttlSeconds, TTLCleanRuleBase ttlCleanRuleBase) {
		configs.computeIfAbsent(persistenceStorageAbstractConfig.getName(), k -> {
			if (ttlCleanRuleBase == null) return new TTLConfig(persistenceStorageAbstractConfig, ttlSeconds);
			return new TTLConfig(persistenceStorageAbstractConfig, ttlSeconds, ttlCleanRuleBase);
		});
		configs.computeIfPresent(persistenceStorageAbstractConfig.getName(), (k, v) -> {
			if (ttlCleanRuleBase != null) v.addTTLCleanRule(ttlCleanRuleBase);
			if (ttlSeconds > 0) v.setTtlSeconds(ttlSeconds);
			return v;
		});
	}

	public void removeTTL(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
		if (null == persistenceStorageAbstractConfig || null == persistenceStorageAbstractConfig.getName()) {
			return;
		}
		configs.remove(persistenceStorageAbstractConfig.getName());
		backlogRounds.remove(persistenceStorageAbstractConfig.getName());
	}

	public void start() {
		if (isRunning.compareAndSet(false, true)) {
			scheduler = new ScheduledThreadPoolExecutor(1, r -> {
				Thread thread = new Thread(r);
				thread.setName(String.join("-", TTLService.class.getName(), "ttl", "scheduler", String.valueOf(System.currentTimeMillis())));
				return thread;
			});
			scheduler.schedule(this::tick, 0L, TimeUnit.MILLISECONDS);
			info("External persistent storage TTL timing service started successfully. Interval: {} ms, backlog interval: {} ms", intervalMs, backlogIntervalMs);
		}
	}

	/**
	 * One scheduling tick: run a round, then always schedule the next one, so the schedule never stops silently.
	 */
	void tick() {
		long nextDelay = intervalMs;
		try {
			RoundResult roundResult = roundTask.call();
			if (null != roundResult) {
				if (roundResult.hasMore) {
					nextDelay = backlogIntervalMs;
				}
				if (roundResult.objects > 0) {
					info("Do ttl round finished, objects: {}, deleted: {}, cost: {} ms, next in: {} ms",
							roundResult.objects, roundResult.deleted, roundResult.costMs, nextDelay);
				}
			}
		} catch (Throwable t) {
			warn("Do ttl round failed, will retry next round, next in: " + nextDelay + " ms", t);
		} finally {
			ScheduledExecutorService current = scheduler;
			if (null != current) {
				try {
					current.schedule(this::tick, nextDelay, TimeUnit.MILLISECONDS);
				} catch (RejectedExecutionException e) {
					warn("Schedule next ttl round failed", e);
				}
			}
		}
	}

	RoundResult doTTL() {
		long startMs = System.currentTimeMillis();
		List<String> names = new ArrayList<>(configs.keySet());
		if (names.isEmpty()) {
			return new RoundResult(0, 0L, false, 0L);
		}
		List<List<String>> partition = partition(names, threadNumber);
		int threadNum = partition.size();
		ExecutorService ttlExecutorService = new ThreadPoolExecutor(threadNum, threadNum, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
				r -> {
					Thread thread = new Thread(r);
					thread.setName("Hazelcast-Persistence-Clear-TTL-Data-Thread-" + System.currentTimeMillis());
					return thread;
				});
		List<TTLRunner> runners = new ArrayList<>();
		try {
			List<CompletableFuture<?>> completableFutures = new ArrayList<>();
			for (List<String> partNames : partition) {
				TTLRunner ttlRunner = new TTLRunner(partNames);
				runners.add(ttlRunner);
				completableFutures.add(CompletableFuture.runAsync(ttlRunner::doTTL, ttlExecutorService));
			}
			CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0])).join();
		} finally {
			ttlExecutorService.shutdown();
			try {
				if (!ttlExecutorService.awaitTermination(1L, TimeUnit.MINUTES)) {
					ttlExecutorService.shutdownNow();
				}
			} catch (InterruptedException e) {
				ttlExecutorService.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
		int objects = 0;
		long deleted = 0L;
		boolean hasMore = false;
		for (TTLRunner runner : runners) {
			objects += runner.objects;
			deleted += runner.deleted;
			hasMore |= runner.hasMore;
		}
		return new RoundResult(objects, deleted, hasMore, System.currentTimeMillis() - startMs);
	}

	/**
	 * Split names into at most {@code threadNum} partitions (ceil division)
	 */
	static List<List<String>> partition(List<String> names, int threadNum) {
		if (CollectionUtils.isEmpty(names)) {
			return Collections.emptyList();
		}
		int threads = Math.max(1, threadNum);
		int size = (names.size() + threads - 1) / threads;
		return ListUtils.partition(names, size);
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create();
		for (Map.Entry<String, TTLConfig> entry : configs.entrySet()) {
			String key = entry.getKey();
			TTLConfig value = entry.getValue();
			AtomicInteger rounds = backlogRounds.get(key);
			dataMap.kv(key, "ttl seconds: " + value.getTtlSeconds() + (null == rounds ? "" : ", backlog rounds: " + rounds.get()));
		}
		return dataMap;
	}

	// ---- package level helpers for tests ----

	void setRoundTask(Callable<RoundResult> roundTask) {
		this.roundTask = null == roundTask ? this::doTTL : roundTask;
	}

	int backlogRounds(String name) {
		AtomicInteger rounds = backlogRounds.get(name);
		return null == rounds ? 0 : rounds.get();
	}

	void stop() {
		ScheduledExecutorService current = scheduler;
		scheduler = null;
		if (null != current) {
			current.shutdownNow();
		}
		isRunning.set(false);
	}

	static void clearConfigs() {
		configs.clear();
	}

	private void info(String message, Object... params) {
		if (null != logger) {
			logger.info(message, params);
		}
	}

	private void warn(String message, Object... params) {
		if (null != logger) {
			logger.warn(message, params);
		}
	}

	private void warn(String message, Throwable t) {
		if (null != logger) {
			logger.warn("{}, error: {}\n{}", message, t.getMessage(), CommonUtils.stackString(t));
		}
	}

	static final class RoundResult {
		final int objects;
		final long deleted;
		final boolean hasMore;
		final long costMs;

		RoundResult(int objects, long deleted, boolean hasMore, long costMs) {
			this.objects = objects;
			this.deleted = deleted;
			this.hasMore = hasMore;
			this.costMs = costMs;
		}
	}

	private class TTLRunner {
		private final List<String> names;
		private int objects;
		private long deleted;
		private boolean hasMore;

		public TTLRunner(List<String> names) {
			this.names = names;
		}

		void doTTL() {
			if (CollectionUtils.isEmpty(names)) {
				return;
			}
			info("Start to do ttl, object size: {}, object names: {}", names.size(), names);
			for (String name : names) {
				try {
					TTLConfig ttlConfig = configs.get(name);
					if (null == ttlConfig || null == ttlConfig.getPersistenceStorageAbstractConfig()) {
						continue;
					}
					PersistenceStorageAbstractConfig storageConfig = ttlConfig.getPersistenceStorageAbstractConfig();
					StorageMode storageMode = storageConfig.getStorageMode();
					// Check before creating the store, otherwise the store would never be destroyed
					if (storageMode == StorageMode.Mem || storageMode == StorageMode.HTTP_TM) {
						continue;
					}
					PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> persistenceStorageStore = createStoreFunc.apply(storageConfig);
					if (null == persistenceStorageStore) {
						continue;
					}
					try {
						TTLProcessor ttlProcessor = processorLookup.apply(storageConfig.getConstructType());
						if (null == ttlProcessor) {
							continue;
						}
						TTLMetrics ttlMetrics = ttlProcessor.doTTL(new TTLProcessorContext(persistenceStorageStore, logger), ttlConfig);
						handleMetrics(name, ttlMetrics);
					} finally {
						persistenceStorageStore.doDestroy();
					}
				} catch (Throwable e) {
					logWarn(name, e);
				}
			}
		}

		private void handleMetrics(String name, TTLMetrics ttlMetrics) {
			if (null == ttlMetrics) {
				return;
			}
			objects++;
			long deleteCount = null == ttlMetrics.getDeleteCount() ? 0L : ttlMetrics.getDeleteCount();
			deleted += deleteCount;
			if (null != ttlMetrics.getError()) {
				logWarn(ttlMetrics.getName(), ttlMetrics.getError());
			} else {
				info("Do ttl success, name: {}, delete count: {}, cost: {} ms, has more: {}",
						ttlMetrics.getName(), deleteCount, ttlMetrics.getCostMs(), ttlMetrics.isHasMore());
			}
			if (ttlMetrics.getBatchRetries() > 0) {
				warn("TTL delete batch failed and was retried with a smaller batch, name: {}, retries: {}, batch size now: {}, last error: {}. " +
								"MongoDB may be slow, consider decreasing {}",
						name, ttlMetrics.getBatchRetries(), ttlMetrics.getBatchSize(), ttlMetrics.getLastBatchError(), "TTL_DELETE_BATCH_SIZE");
			}
			if (ttlMetrics.isHasMore()) {
				hasMore = true;
				int rounds = backlogRounds.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
				if (rounds >= BACKLOG_WARN_ROUNDS) {
					warn("TTL backlog is not draining, name: {}, consecutive backlog rounds: {}, deleted this round: {}. " +
									"Deletion cannot keep up with writes, consider increasing {} or {}",
							name, rounds, deleteCount, "TTL_DELETE_BATCH_SIZE", "TTL_MAX_DURATION_PER_OBJECT_MS");
				}
			} else {
				backlogRounds.remove(name);
			}
		}

		private void logWarn(String name, Throwable e) {
			if (null != logger) {
				logger.warn("Do ttl failed, name: {}, error: {}\n{}", name, e.getMessage(), CommonUtils.stackString(e));
			}
		}
	}
}
