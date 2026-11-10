package com.hazelcast.persistence.store;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.ttl.RingBufferExpireResult;
import com.hazelcast.ringbuffer.RingbufferStore;
import org.bson.Document;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 19:43
 **/
public abstract class PersistenceRingBufferStore<T extends PersistenceStorageAbstractConfig, R extends ExternalResource<T>> extends PersistenceStorageStore<T, R>
		implements RingbufferStore<Object> {
	protected String ringBufferName;
	protected final static Document EMPTY_DOCUMENT = new Document();
	public static final String SIGN_TYPE = "SIGN";
	private static final String VALUE_FIELD = "value";

	@Override
	public void doInit(T t, R r) {
		super.doInit(t, r);
		this.ringBufferName = t.getName();
	}

	abstract public void delete(long s);

	abstract public long getSmallestSequence();

	abstract public long findSequenceByTimestamp(long timestamp);

	/**
	 * Smallest sequence that is not a SIGN record. Default implementation starts from the smallest stored sequence,
	 * so stores that do not override it do not rescan already deleted sequences from 0 every TTL round.
	 */
	public long getSmallestSequenceWithoutSign() {
		return Math.max(0L, getSmallestSequence());
	}

	/**
	 * Delete one batch of consecutive expired records starting from {@code fromSequence}.
	 * <p>
	 * Expired means {@code value._ts <= expireBeforeSec}. SIGN records, records without a numeric {@code _ts},
	 * and records whose sequence is {@code >= maxSequenceExclusive} are never deleted.
	 * Stops at the first unexpired record.
	 * <p>
	 * Default implementation keeps the per-sequence behavior ({@link #load(long)} + {@link #delete(long)}),
	 * handling at most {@code batchSize} sequences per call.
	 *
	 * @param fromSequence         start sequence (inclusive)
	 * @param maxSequenceExclusive upper bound (exclusive), usually the largest sequence which must be kept
	 * @param expireBeforeSec      cutoff in seconds, records with {@code _ts <= expireBeforeSec} are expired
	 * @param batchSize            max number of sequences (or records) handled by this call, must be positive
	 */
	public RingBufferExpireResult deleteExpired(long fromSequence, long maxSequenceExclusive, long expireBeforeSec, int batchSize) {
		if (batchSize <= 0) {
			throw new IllegalArgumentException("Batch size must be positive: " + batchSize);
		}
		long sequence = Math.max(0L, fromSequence);
		long deleted = 0L;
		for (int processed = 0; processed < batchSize; processed++, sequence++) {
			if (sequence >= maxSequenceExclusive) {
				return new RingBufferExpireResult(deleted, sequence, true);
			}
			Object obj;
			try {
				obj = load(sequence);
			} catch (Exception e) {
				throw new RuntimeException("Read one from ringBuffer failed, sequence: " + sequence, e);
			}
			if (!(obj instanceof Map<?, ?> document) || isSign(document)) {
				continue;
			}
			Long ts = readTs(document);
			if (null == ts) {
				continue;
			}
			if (ts > expireBeforeSec) {
				return new RingBufferExpireResult(deleted, sequence, true);
			}
			delete(sequence);
			deleted++;
		}
		return new RingBufferExpireResult(deleted, sequence, sequence >= maxSequenceExclusive);
	}

	/**
	 * @param value the ring buffer value document (what {@link #load(long)} returns)
	 * @return true when it is a SIGN record
	 */
	public static boolean isSign(Map<?, ?> value) {
		return null != value && SIGN_TYPE.equals(value.get("type"));
	}

	/**
	 * Read {@code _ts} (seconds), same lookup as the previous TTL implementation: the document itself,
	 * or its {@code value} sub document when present. Compatible with Int32/Int64/other numbers.
	 *
	 * @return null when absent or not a number
	 */
	public static Long readTs(Map<?, ?> document) {
		Map<?, ?> value = unwrapValue(document);
		return null == value ? null : toLong(value.get("_ts"));
	}

	public static Long toLong(Object obj) {
		return obj instanceof Number number ? number.longValue() : null;
	}

	private static Map<?, ?> unwrapValue(Map<?, ?> document) {
		if (null == document) {
			return null;
		}
		Object value = document.get(VALUE_FIELD);
		return value instanceof Map<?, ?> map ? map : document;
	}
}
