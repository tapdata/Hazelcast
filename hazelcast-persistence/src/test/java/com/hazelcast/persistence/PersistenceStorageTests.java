package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.*;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.config.*;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.commons.lang3.RandomStringUtils;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * @author samuel
 * @Description
 * @create 2023-02-07 15:56
 **/
public class PersistenceStorageTests {

	private static HazelcastInstance hazelcastInstance;
	private static PersistenceStorage persistenceStorage;

	@BeforeAll
	public static void init() {
		Config config = new Config();
		config.getJetConfig().setEnabled(true);
		JoinConfig joinConfig = new JoinConfig();
		joinConfig.setTcpIpConfig(new TcpIpConfig().setEnabled(true));
		NetworkConfig networkConfig = new NetworkConfig();
		networkConfig.setJoin(joinConfig);
		config.setNetworkConfig(networkConfig);
		config.setInstanceName("test");
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		persistenceStorage = PersistenceStorage.getInstance();
	}

	@AfterAll
	public static void afterAll() {
		try {
			hazelcastInstance.shutdown();
		} catch (Exception ignored) {
		}
	}

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

		PersistenceHttpConfig imapHttpConfig = PersistenceHttpConfig.create(ConstructType.IMAP, "http-imap", Collections.singletonList("http://127.0.0.1:8080/api/"), "abc");
		persistenceStorage.addConfig(imapHttpConfig);
		persistenceStorageAbstractConfig = persistenceStorage.getPersistenceStorageConfig(imapHttpConfig.getConstructType(), imapHttpConfig.getName());
		Assertions.assertEquals(PersistenceHttpConfig.class.getName(), persistenceStorageAbstractConfig.getClass().getName());
		System.out.println(persistenceStorageAbstractConfig);
		PersistenceHttpConfig persistenceHttpConfig = (PersistenceHttpConfig) persistenceStorageAbstractConfig;
//		Assertions.assertEquals("http://127.0.0.1:8080/api/", persistenceHttpConfig.getBaseUrl());
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
		Assertions.assertFalse(PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap").equals(PersistenceHttpConfig.create(ConstructType.IMAP, "imap", Collections.singletonList(""), "")));
		Assertions.assertTrue(PersistenceRocksDBConfig.create(ConstructType.RINGBUFFER, "ringbuffer").equals(PersistenceRocksDBConfig.create(ConstructType.RINGBUFFER, "ringbuffer")));
		Assertions.assertFalse(PersistenceInMemConfig.create(ConstructType.IMAP).equals(PersistenceInMemConfig.create(ConstructType.RINGBUFFER)));
	}

	public static void main(String[] args) throws Throwable {
		Config config = new Config();
		config.getJetConfig().setEnabled(true);
		JoinConfig joinConfig = new JoinConfig();
		joinConfig.setTcpIpConfig(new TcpIpConfig().setEnabled(true));
		NetworkConfig networkConfig = new NetworkConfig();
		networkConfig.setJoin(joinConfig);
		config.setNetworkConfig(networkConfig);
		config.setInstanceName("test");
		HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		JetService jet = hazelcastInstance.getJet();
		DAG dag = new DAG();
		Vertex src = new Vertex("src", DummySourceProcessor::new).localParallelism(1);
		Vertex tgt = new Vertex("tgt", DummyTargetProcessor::new).localParallelism(1);
		dag.vertex(src);
		dag.vertex(tgt);
		Edge edge = Edge.between(src, tgt);
		dag.edge(edge);
		Job job = jet.newJob(dag);
		TimeUnit.SECONDS.sleep(2L);
		job.cancel();
		TimeUnit.SECONDS.sleep(2L);
		hazelcastInstance.shutdown();

		/*Config config = new Config();
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
		}*/
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

	@Test
	public void test999() {
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "test999")
				.uri("mongodb://localhost/test")
				.database("test")
				.collection("test999");
		mongoDBConfig.setInMemSize(1000);
		mongoDBConfig.setWriteDelaySeconds(1);

		PersistenceStorage.getInstance().addConfig(mongoDBConfig).initMapStoreConfig(hazelcastInstance.getConfig(), mongoDBConfig.getName());
		PersistenceStorage.getInstance().clear(ConstructType.IMAP, mongoDBConfig.getName());
		IMap<String, Object> map = hazelcastInstance.getMap(mongoDBConfig.getName());
//		IntStream.range(1, 301).forEach(i -> {
//			map.put(String.valueOf(i), new Document().append("name", String.valueOf(i)));
//		});
//		PersistenceStorage.getInstance().clear(ConstructType.IMAP, mongoDBConfig.getName());
//		PersistenceStorage.getInstance().destroy(ConstructType.IMAP, mongoDBConfig.getName());
//		persistenceStorage.addConfig(mongoDBConfig).initMapStoreConfig(hazelcastInstance.getConfig(), mongoDBConfig.getName());
		Map<String, Object> cache = new HashMap<>();
		IntStream.range(1, 30001).forEach(i -> {
			cache.put(String.valueOf(i), new Document("name", String.valueOf(i)));
		});
		map.putAll(cache);
	}

	@Test
	public void test888() {
		try (MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost/test"))) {
			MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("test888");
			int testNum = 100000;
			int time = 1;

			while (true) {
				collection.drop();
				PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.RINGBUFFER, "test888")
						.uri("mongodb://localhost/test")
						.database("test")
						.collection("test888");
				mongoDBConfig.setInMemSize(1);
				mongoDBConfig.setWriteDelaySeconds(1);

				PersistenceStorage.getInstance().addConfig(mongoDBConfig).initRingBufferConfig(hazelcastInstance.getConfig(), mongoDBConfig.getName());
				PersistenceStorage.getInstance().clear(ConstructType.RINGBUFFER, mongoDBConfig.getName());
				Ringbuffer<Document> ringbuffer = hazelcastInstance.getRingbuffer(mongoDBConfig.getName());
				List<Document> list = new ArrayList<>();
				IntStream.range(1, testNum + 1).forEach(i -> {
					list.add(new Document("id", String.valueOf(i)));
					if (list.size() >= 1000) {
						ringbufferAddAll(ringbuffer, list);
						list.clear();
					}
				});
				if (!list.isEmpty()) {
					ringbufferAddAll(ringbuffer, list);
				}

				long count = collection.count();
				if (count < testNum) {
					System.out.println("Found skip: " + (testNum - count) + ", time: " + time);
				} else {
					System.out.println("Finish test time: " + time);
				}
				time++;
				/*Document first = collection.find(new Document("ringBuffer", "test888")).sort(new Document("key", 1)).limit(1).iterator().next();
				Document last = collection.find(new Document("ringBuffer", "test888")).sort(new Document("key", -1)).limit(1).iterator().next();
				if (last.getLong("key") - first.getLong("key") != 2999998) {
					System.out.println("skip");
				}*/
			}
		}
	}

	private static void ringbufferAddAll(Ringbuffer<Document> ringbuffer, List<Document> list) {
		CountDownLatch countDownLatch = new CountDownLatch(1);
		ringbuffer.addAllAsync(list, OverflowPolicy.OVERWRITE).whenComplete((r, e) -> {
			try {
				if (null != e) {
					e.printStackTrace();
				}
			} finally {
				countDownLatch.countDown();
			}
		});
		while (true) {
			try {
				if (countDownLatch.await(10L, TimeUnit.MILLISECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}

}
