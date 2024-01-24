package com.hazelcast.persistence.store.ttl;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.StorageMode;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.PDKIntegration;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
	private final AtomicBoolean isRunning = new AtomicBoolean();
	private final Function<PersistenceStorageAbstractConfig, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>> createStoreFunc;
	private final Logger logger;

	public TTLService(
			Function<PersistenceStorageAbstractConfig, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>> createStoreFunc,
			Logger logger
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
		PDKIntegration.registerMemoryFetcher(TTLService.class.getSimpleName(), this);
	}

	public TTLConfig registerTTL(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig,long ttlSeconds,TTLCleanRuleBase ttlCleanRuleBase) {
		configs.computeIfAbsent(persistenceStorageAbstractConfig.getName(), k ->{
			if(ttlCleanRuleBase == null)return new TTLConfig(persistenceStorageAbstractConfig, ttlSeconds);
			return new TTLConfig(persistenceStorageAbstractConfig,ttlSeconds,ttlCleanRuleBase);
		});
		configs.computeIfPresent(persistenceStorageAbstractConfig.getName(), (k, v) -> {
			if(ttlCleanRuleBase != null) v.addTTLCleanRule(ttlCleanRuleBase);
			if(ttlSeconds > 0) v.setTtlSeconds(ttlSeconds);
			return v;
		});
		return configs.get(persistenceStorageAbstractConfig.getName());
	}

	public TTLConfig removeTTL(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
		return configs.remove(persistenceStorageAbstractConfig.getName());
	}

	public void start() {
		if (isRunning.compareAndSet(false, true)) {
			ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
			scheduledExecutorService.scheduleWithFixedDelay(this::doTTL, TTL_INTERVAL_MS, TTL_INTERVAL_MS, TimeUnit.MILLISECONDS);
		}
	}

	private void doTTL() {
		int ttlThreadNum = Math.min(Runtime.getRuntime().availableProcessors(), DEFAULT_TTL_PROCESS_THREAD_NUMBER);
		int size = Math.max(1, configs.size() / ttlThreadNum);
		List<List<String>> partition = ListUtils.partition(new ArrayList<>(configs.keySet()), size);
		ttlThreadNum = partition.size();
		ExecutorService ttlExecutorService = new ThreadPoolExecutor(ttlThreadNum, ttlThreadNum, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
				r -> {
					Thread thread = new Thread(r);
					thread.setName("Hazelcast-Persistence-Clear-TTL-Data-Thread-" + System.currentTimeMillis());
					return thread;
				});
		try {
			List<CompletableFuture<?>> completableFutures = new ArrayList<>();
			for (List<String> names : partition) {
				TTLRunner ttlRunner = new TTLRunner(names);
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
			}
		}
	}

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create();
		for (Map.Entry<String, TTLConfig> entry : configs.entrySet()) {
			String key = entry.getKey();
			TTLConfig value = entry.getValue();
			dataMap.kv(key, "ttl seconds: " + value.getTtlSeconds());
		}
		return dataMap;
	}

	private class TTLRunner {
		private final List<String> names;

		public TTLRunner(List<String> names) {
			this.names = names;
		}

		void doTTL() {
			for (String name : names) {
				try {
					TTLConfig ttlConfig = configs.get(name);
					if (null == ttlConfig) {
						continue;
					}
					PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> persistenceStorageStore = createStoreFunc.apply(ttlConfig.getPersistenceStorageAbstractConfig());
					if (null == persistenceStorageStore) {
						return;
					}
					StorageMode storageMode = ttlConfig.getPersistenceStorageAbstractConfig().getStorageMode();
					if (storageMode == StorageMode.Mem || storageMode == StorageMode.HTTP_TM) {
						return;
					}
					TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(persistenceStorageStore, logger);
					try {
						TTL_PROCESSOR_MAP.get(ttlConfig.getPersistenceStorageAbstractConfig().getConstructType().name()).doTTL(ttlProcessorContext, ttlConfig);
					} finally {
						persistenceStorageStore.doDestroy();
					}
				} catch (Exception e) {
					if (null != logger) {
						logger.warn("Do ttl failed, name: " + name, e);
					}
				}
			}
		}
	}
}
