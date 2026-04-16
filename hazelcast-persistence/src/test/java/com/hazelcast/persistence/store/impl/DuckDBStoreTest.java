package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceDuckDBConfig;
import com.hazelcast.persistence.resource.impl.DuckDBResource;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DuckDBStoreTest {

	@Test
	void imapStoreLoadAndIterate_inMemory() {
		PersistenceDuckDBConfig config = PersistenceDuckDBConfig.create(ConstructType.IMAP, "imap-test");
		DuckDBResource resource = new DuckDBResource();
		resource.doInit(config);

		DuckDBIMap store = new DuckDBIMap();
		store.doInit(config, resource);

		Document v1 = new Document("a", 1);
		store.store("k1", v1);
		Document loaded = store.load("k1");
		Assertions.assertNotNull(loaded);
		Assertions.assertEquals(1, loaded.getInteger("a"));
		Assertions.assertTrue(loaded.containsKey("_ts"));

		Map<String, Object> batch = new HashMap<>();
		batch.put("k2", new Document("b", "x"));
		batch.put("k3", new Document("c", 3));
		store.storeAll(batch);
		Map<String, Object> loadedAll = store.loadAll(batch.keySet());
		Assertions.assertNotNull(loadedAll);
		Assertions.assertEquals(2, loadedAll.size());

		int[] count = new int[]{0};
		Iterable<Document> it = store.iterator();
		Assertions.assertNotNull(it);
		it.forEach(doc -> {
			Assertions.assertTrue(doc.containsKey("key"));
			Assertions.assertTrue(doc.containsKey("value"));
			count[0]++;
		});
		Assertions.assertTrue(count[0] >= 3);

		store.delete("k1");
		Assertions.assertNull(store.load("k1"));

		store.doDestroy();
	}

	@Test
	void ringbufferStoreLoadAndDelete_inMemory() {
		PersistenceDuckDBConfig config = PersistenceDuckDBConfig.create(ConstructType.RINGBUFFER, "rb-test");
		config.sequenceMode(PersistenceStorage.SequenceMode.STORE);
		DuckDBResource resource = new DuckDBResource();
		resource.doInit(config);

		DuckDBRingBuffer store = new DuckDBRingBuffer();
		store.doInit(config, resource);

		store.store(0L, new Document("timestamp", 1000L).append("type", "DATA"));
		store.store(1L, new Document("timestamp", 2000L).append("type", "DATA"));
		store.store(2L, new Document("timestamp", 3000L).append("type", "DATA"));

		Assertions.assertEquals(2L, store.getLargestSequence());
		Assertions.assertEquals(0L, store.getSmallestSequence());

		Document d0 = store.load(0L);
		Assertions.assertNotNull(d0);
		Assertions.assertEquals(1000L, ((Number) d0.get("timestamp")).longValue());

		long seq = store.findSequenceByTimestamp(2000L);
		Assertions.assertEquals(1L, seq);

		store.delete(0L);
		Assertions.assertNull(store.load(0L));
		Assertions.assertEquals(1L, store.getSmallestSequence());

		store.doDestroy();
	}
}
