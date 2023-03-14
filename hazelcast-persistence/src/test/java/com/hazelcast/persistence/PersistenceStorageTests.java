package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.persistence.config.PersistenceHttpConfig;
import com.hazelcast.persistence.config.PersistenceInMemConfig;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.sql.impl.schema.TableResolver;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 15:56
 **/
public class PersistenceStorageTests {

	@Test
	public void addConfigTest() {
		PersistenceStorage persistenceStorage = PersistenceStorage.getInstance();
		PersistenceMongoDBConfig imapMongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap")
				.uri("mongodb://root:Gotapd8!@139.198.127.204:32550/qa?authSource=admin")
				.database("hazelcast")
				.collection("imap_default_config");
		persistenceStorage.addConfig(imapMongoDBConfig);
		PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = persistenceStorage.getPersistenceStorageConfig(imapMongoDBConfig.getConstructType(), imapMongoDBConfig.getName());
		Assertions.assertEquals(PersistenceMongoDBConfig.class.getName(), persistenceStorageAbstractConfig.getClass().getName());
		PersistenceMongoDBConfig persistenceMongoDBConfig = (PersistenceMongoDBConfig) persistenceStorageAbstractConfig;
		Assertions.assertEquals("mongodb://root:Gotapd8!@139.198.127.204:32550/qa?authSource=admin", persistenceMongoDBConfig.getUri());
		Assertions.assertEquals("hazelcast", persistenceMongoDBConfig.getDatabase());
		Assertions.assertEquals("imap_default_config", persistenceMongoDBConfig.getCollection());
		System.out.println(persistenceStorageAbstractConfig);

		PersistenceRocksDBConfig ringBufferRocksDBConfig = PersistenceRocksDBConfig.create(ConstructType.RINGBUFFER, "rocksdb-ringbuffer")
				.path("./rocksdb-ringbuffer");
		ringBufferRocksDBConfig.setInMemSize(100);
		persistenceStorage.addConfig(ringBufferRocksDBConfig);
		persistenceStorageAbstractConfig = persistenceStorage.getPersistenceStorageConfig(ringBufferRocksDBConfig.getConstructType(), ringBufferRocksDBConfig.getName());
		Assertions.assertEquals(PersistenceRocksDBConfig.class.getName(), persistenceStorageAbstractConfig.getClass().getName());
		PersistenceRocksDBConfig persistenceRocksDBConfig = (PersistenceRocksDBConfig) persistenceStorageAbstractConfig;
		Assertions.assertEquals(100, persistenceRocksDBConfig.getInMemSize());
		Assertions.assertEquals("./rocksdb-ringbuffer", persistenceRocksDBConfig.getPath());
		System.out.println(persistenceStorageAbstractConfig);

		PersistenceHttpConfig imapHttpConfig = PersistenceHttpConfig.create(ConstructType.IMAP, "http-imap", "http://127.0.0.1:8080/api/", "abc");
		persistenceStorage.addConfig(imapHttpConfig);
		persistenceStorageAbstractConfig = persistenceStorage.getPersistenceStorageConfig(imapHttpConfig.getConstructType(), imapHttpConfig.getName());
		Assertions.assertEquals(PersistenceHttpConfig.class.getName(), persistenceStorageAbstractConfig.getClass().getName());
		System.out.println(persistenceStorageAbstractConfig);
		PersistenceHttpConfig persistenceHttpConfig = (PersistenceHttpConfig) persistenceStorageAbstractConfig;
		Assertions.assertEquals("http://127.0.0.1:8080/api/", persistenceHttpConfig.getBaseUrl());
		Assertions.assertEquals("abc", persistenceHttpConfig.getAccessCode());

		PersistenceRocksDBConfig imapRocksDBConfig = PersistenceRocksDBConfig.create(ConstructType.IMAP, "imap");
		try {
			persistenceStorage.addConfig(imapRocksDBConfig);
		} catch (Exception e) {
			Assertions.assertEquals("Change persistence storage mode is not allowed\n" +
					" old: PersistenceMongoDBConfig[constructType=IMAP, name='imap', storageMode=MongoDB, inMemSize=1, uri='mongodb://root:******@127.0.0.1:27017', database='hazelcast', collection='imap_default_config']\n" +
					" new: PersistenceRocksDBConfig[constructType=IMAP, name='imap', storageMode=RocksDB, inMemSize=1, path='./tap_default_rocksdb_cache']", e.getMessage());
		}
	}

	@Test
	public void configEqualsTest() {
		Assertions.assertTrue(PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap").equals(PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap")));
		Assertions.assertFalse(PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap").equals(PersistenceMongoDBConfig.create(ConstructType.RINGBUFFER, "ringbuffer")));
		Assertions.assertFalse(PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap").equals(PersistenceHttpConfig.create(ConstructType.IMAP, "imap", "", "")));
		Assertions.assertTrue(PersistenceRocksDBConfig.create(ConstructType.RINGBUFFER, "ringbuffer").equals(PersistenceRocksDBConfig.create(ConstructType.RINGBUFFER, "ringbuffer")));
		Assertions.assertFalse(PersistenceInMemConfig.create(ConstructType.IMAP).equals(PersistenceInMemConfig.create(ConstructType.RINGBUFFER)));
	}

	public static void main(String[] args) throws Throwable {
		Config config = new Config();
		config.getJetConfig().setEnabled(true);
		PersistenceStorage persistenceStorage = PersistenceStorage.getInstance();
		PersistenceMongoDBConfig rbMongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.RINGBUFFER, "rb")
				.uri("mongodb://root:Gotapd8!@139.198.127.204:32550/qa?authSource=admin")
				.database("hazelcast")
				.collection("ringBuffer_default_config");
		PersistenceMongoDBConfig imapMongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap")
				.uri("mongodb://root:Gotapd8!@139.198.127.204:32550/qa?authSource=admin")
				.database("hazelcast")
				.collection("imap_default_config");
		persistenceStorage.addConfig(rbMongoDBConfig);
		persistenceStorage.addConfig(imapMongoDBConfig);
		persistenceStorage.initRingBufferConfig(config, "rb");
		persistenceStorage.initMapStoreConfig(config, "imap");
		HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
		Ringbuffer<Document> rb = hz.getRingbuffer("rb");
		rb.add(new Document().append("x", 1).append("y", "sss"));
		rb.add(new Document().append("x", 2).append("y", "sss"));
		while (true) {
			rb.tailSequence();
		}
		//System.out.println(rb.readOne(rb.tailSequence()));
	}

	static class DummySourceProcessor extends AbstractProcessor {
		private final AtomicLong id = new AtomicLong(0L);

		@Override
		public boolean complete() {
			CommonUtils.ignoreAnyError(() -> TimeUnit.MILLISECONDS.sleep(500L));
			Map<String, Object> map = new HashMap<>();
			map.put("id", id.incrementAndGet());
			map.put("name", RandomStringUtils.randomAlphabetic(10));
			while (!Thread.currentThread().isInterrupted()) {
				if (tryEmit(map)) {
					break;
				}
				CommonUtils.ignoreAnyError(() -> TimeUnit.MILLISECONDS.sleep(1L));
			}
			return false;
		}

		@Override
		public void close() throws Exception {
			Thread.currentThread().interrupt();
			super.close();
		}
	}

	static class DummyTargetProcessor extends AbstractProcessor {
		@Override
		public void process(int ordinal, Inbox inbox) {
			ArrayList<Object> list = new ArrayList<>();
			int count = inbox.drainTo(list, 10);
			if (count > 0) {
				list.forEach(System.out::println);
			}
		}
	}

}
