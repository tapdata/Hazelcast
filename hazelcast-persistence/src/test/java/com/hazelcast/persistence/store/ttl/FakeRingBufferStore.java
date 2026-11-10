package com.hazelcast.persistence.store.ttl;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory ring buffer store for TTL tests; does not override deleteExpired, so it exercises the base default path.
 */
public class FakeRingBufferStore extends PersistenceRingBufferStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> {
	public final TreeMap<Long, Document> data = new TreeMap<>();
	public final List<Long> deleted = new ArrayList<>();
	public final AtomicInteger loadCount = new AtomicInteger();
	public final AtomicInteger destroyCount = new AtomicInteger();
	/**
	 * Simulates stores that keep a persisted smallest sequence (like RocksDB), updated on delete
	 */
	public long smallestSequence = 0L;
	public long largestSequence = -1L;

	public FakeRingBufferStore put(long key, Document value) {
		data.put(key, value);
		largestSequence = Math.max(largestSequence, key);
		return this;
	}

	public static Document value(Object ts) {
		Document doc = new Document("type", "DATA");
		if (null != ts) {
			doc.append("_ts", ts);
		}
		return doc;
	}

	public static Document sign() {
		return new Document("type", "SIGN").append("_ts", 1L);
	}

	@Override
	public void delete(long s) {
		data.remove(s);
		deleted.add(s);
		smallestSequence = s + 1;
	}

	@Override
	public long getSmallestSequence() {
		return smallestSequence;
	}

	@Override
	public long findSequenceByTimestamp(long timestamp) {
		return 0;
	}

	@Override
	public void doDestroy() {
		destroyCount.incrementAndGet();
	}

	@Override
	public void store(long sequence, Object value) {
		put(sequence, (Document) value);
	}

	@Override
	public void storeAll(long firstItemSequence, Object[] items) {
		for (int i = 0; i < items.length; i++) {
			store(firstItemSequence + i, items[i]);
		}
	}

	@Override
	public Object load(long sequence) {
		loadCount.incrementAndGet();
		return data.get(sequence);
	}

	@Override
	public long getLargestSequence() {
		return largestSequence;
	}

	public Map<Long, Document> remaining() {
		return data;
	}
}
