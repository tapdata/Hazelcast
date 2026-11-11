package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.ttl.RingBufferExpireResult;
import com.hazelcast.persistence.store.ttl.TTLConfig;
import com.hazelcast.persistence.store.ttl.TTLMetrics;
import com.hazelcast.persistence.store.ttl.TTLProcessorContext;

import java.util.concurrent.TimeUnit;

/**
 * Ring buffer TTL: delete expired data in batches from the smallest non-SIGN sequence, keep the largest sequence.
 * Each object has a per-round duration limit, data left over is reported by {@link TTLMetrics#isHasMore()}.
 * A failed batch (e.g. socket timeout on a slow MongoDB) is retried from the same sequence with half the batch size,
 * down to {@link #MIN_BATCH_SIZE}; the delete filter only matches expired data, so a retry is always safe.
 *
 * @author samuel
 * @Description
 * @create 2023-09-21 17:37
 **/
public class RingBufferTTLProcessor extends BaseTTLProcessor {
	public static final String BATCH_SIZE_KEY = "TTL_DELETE_BATCH_SIZE";
	public static final int DEFAULT_BATCH_SIZE = 5000;
	public static final int MAX_BATCH_SIZE = 10000;
	public static final int MIN_BATCH_SIZE = 100;
	public static final String BATCH_INTERVAL_MS_KEY = "TTL_DELETE_BATCH_INTERVAL_MS";
	public static final long DEFAULT_BATCH_INTERVAL_MS = 0L;
	public static final String MAX_DURATION_PER_OBJECT_MS_KEY = "TTL_MAX_DURATION_PER_OBJECT_MS";
	public static final long DEFAULT_MAX_DURATION_PER_OBJECT_MS = TimeUnit.MINUTES.toMillis(10L);

	@Override
	public TTLMetrics doTTL(TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig) {
		int batchSize = CommonUtils.getPropertyInt(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE);
		long batchIntervalMs = CommonUtils.getPropertyLong(BATCH_INTERVAL_MS_KEY, DEFAULT_BATCH_INTERVAL_MS);
		long maxDurationMs = CommonUtils.getPropertyLong(MAX_DURATION_PER_OBJECT_MS_KEY, DEFAULT_MAX_DURATION_PER_OBJECT_MS);
		return doTTL(ttlProcessorContext, ttlConfig,
				batchSize > 0 ? Math.min(batchSize, MAX_BATCH_SIZE) : DEFAULT_BATCH_SIZE,
				Math.max(0L, batchIntervalMs),
				maxDurationMs >= 0 ? maxDurationMs : DEFAULT_MAX_DURATION_PER_OBJECT_MS);
	}

	TTLMetrics doTTL(TTLProcessorContext ttlProcessorContext, TTLConfig ttlConfig, int batchSize, long batchIntervalMs, long maxDurationMs) {
		TTLMetrics ttlMetrics = new TTLMetrics(ttlConfig.getPersistenceStorageAbstractConfig().getName());
		long ttlMillis = TimeUnit.SECONDS.toMillis(ttlConfig.getTtlSeconds());
		long startMs = System.currentTimeMillis();
		PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = ttlProcessorContext.getStore();
		if (!(store instanceof PersistenceRingBufferStore)) {
			return ttlMetrics;
		}
		PersistenceRingBufferStore<?, ?> ringBufferStore = (PersistenceRingBufferStore<?, ?>) store;
		ttlMetrics.setBatchSize(batchSize);
		try {
			// The largest sequence itself is never deleted, otherwise sequences restart from 0 after the task restarts
			long maxSequenceExclusive = ringBufferStore.getLargestSequence();
			if (maxSequenceExclusive < 0) {
				return ttlMetrics;
			}
			// Same as previous check: now - _ts * 1000 >= ttlMillis  <=>  _ts <= floor((now - ttlMillis) / 1000)
			long expireBeforeSec = Math.floorDiv(startMs - ttlMillis, 1000L);
			long from = ringBufferStore.getSmallestSequenceWithoutSign();
			while (true) {
				if (Thread.currentThread().isInterrupted()) {
					ttlMetrics.setHasMore(true);
					break;
				}
				RingBufferExpireResult result;
				try {
					result = ringBufferStore.deleteExpired(from, maxSequenceExclusive, expireBeforeSec, batchSize);
				} catch (RuntimeException e) {
					if (batchSize <= MIN_BATCH_SIZE || System.currentTimeMillis() - startMs >= maxDurationMs) {
						throw e;
					}
					batchSize = Math.max(MIN_BATCH_SIZE, batchSize / 2);
					ttlMetrics.setBatchRetries(ttlMetrics.getBatchRetries() + 1);
					ttlMetrics.setBatchSize(batchSize);
					ttlMetrics.setLastBatchError(e.getMessage());
					continue;
				}
				ttlMetrics.setDeleteCount(ttlMetrics.getDeleteCount() + result.getDeletedCount());
				if (result.isFinished()) {
					break;
				}
				if (System.currentTimeMillis() - startMs >= maxDurationMs) {
					ttlMetrics.setHasMore(true);
					break;
				}
				from = Math.max(from + 1, result.getNextSequence());
				if (batchIntervalMs > 0) {
					try {
						TimeUnit.MILLISECONDS.sleep(batchIntervalMs);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						ttlMetrics.setHasMore(true);
						break;
					}
				}
			}
		} catch (Exception e) {
			ttlMetrics.setError(e);
		} finally {
			ttlMetrics.setCostMs(System.currentTimeMillis() - startMs);
		}
		return ttlMetrics;
	}
}
