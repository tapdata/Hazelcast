package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.persistence.config.*;
import com.hazelcast.persistence.store.ttl.TTLCleanMode;
import io.jsonwebtoken.lang.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

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
				.uri("mongodb://root:pwd@139.198.127.204:32550/qa?authSource=admin")
				.database("hazelcast")
				.collection("imap_default_config");
		persistenceStorage.addConfig(imapMongoDBConfig);
		PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = persistenceStorage.getPersistenceStorageConfig(imapMongoDBConfig.getConstructType(), imapMongoDBConfig.getName());
		Assertions.assertEquals(PersistenceMongoDBConfig.class.getName(), persistenceStorageAbstractConfig.getClass().getName());
		PersistenceMongoDBConfig persistenceMongoDBConfig = (PersistenceMongoDBConfig) persistenceStorageAbstractConfig;
		Assertions.assertEquals("mongodb://root:pwd@139.198.127.204:32550/qa?authSource=admin", persistenceMongoDBConfig.getUri());
		Assertions.assertEquals("hazelcast", persistenceMongoDBConfig.getDatabase());
		Assertions.assertEquals("imap_default_config", persistenceMongoDBConfig.getCollection());

		PersistenceRocksDBConfig ringBufferRocksDBConfig = PersistenceRocksDBConfig.create(ConstructType.RINGBUFFER, "rocksdb-ringbuffer")
				.path("./rocksdb-ringbuffer");
		ringBufferRocksDBConfig.setInMemSize(100);
		persistenceStorage.addConfig(ringBufferRocksDBConfig);
		persistenceStorageAbstractConfig = persistenceStorage.getPersistenceStorageConfig(ringBufferRocksDBConfig.getConstructType(), ringBufferRocksDBConfig.getName());
		Assertions.assertEquals(PersistenceRocksDBConfig.class.getName(), persistenceStorageAbstractConfig.getClass().getName());
		PersistenceRocksDBConfig persistenceRocksDBConfig = (PersistenceRocksDBConfig) persistenceStorageAbstractConfig;
		Assertions.assertEquals(100, persistenceRocksDBConfig.getInMemSize());
		Assertions.assertEquals("./rocksdb-ringbuffer", persistenceRocksDBConfig.getPath());

		PersistenceHttpConfig imapHttpConfig = PersistenceHttpConfig.create(ConstructType.IMAP, "http-imap", Collections.singletonList("http://127.0.0.1:8080/api/"), "abc");
		persistenceStorage.addConfig(imapHttpConfig);
		persistenceStorageAbstractConfig = persistenceStorage.getPersistenceStorageConfig(imapHttpConfig.getConstructType(), imapHttpConfig.getName());
		Assertions.assertEquals(PersistenceHttpConfig.class.getName(), persistenceStorageAbstractConfig.getClass().getName());
		PersistenceHttpConfig persistenceHttpConfig = (PersistenceHttpConfig) persistenceStorageAbstractConfig;
		Assertions.assertEquals("abc", persistenceHttpConfig.getAccessCode());

		PersistenceRocksDBConfig imapRocksDBConfig = PersistenceRocksDBConfig.create(ConstructType.IMAP, "imap");
		try {
			persistenceStorage.addConfig(imapRocksDBConfig);
		} catch (Exception e) {
			Assertions.assertEquals("Change persistence storage mode is not allowed until restart instance\n" +
					" old: PersistenceMongoDBConfig[constructType=IMAP, name='imap', storageMode=MongoDB, inMemSize=100, uri='mongodb://root:******@139.198.127.204:32550/qa?authSource=admin', database='hazelcast', collection='imap_default_config', exclusiveCollection=false, ssl=false, sslCA='null', sslKey='null', sslPass='null', sslValidate=false, checkServerIdentity=false]\n" +
					" new: PersistenceRocksDBConfig[constructType=IMAP, name='imap', storageMode=RocksDB, inMemSize=100, path='./tap_default_rocksdb_cache']", e.getMessage());
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

	@Test
	public void setImapTTLTest() throws Exception {
		PersistenceMongoDBConfig imapMongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap")
				.uri("mongodb://root:pwd@139.198.127.204:32550/qa?authSource=admin")
				.database("hazelcast")
				.collection("imap_default_config");
		persistenceStorage.addConfig(imapMongoDBConfig);
		PersistenceStorage resultPersistenceStorage = persistenceStorage.setImapTTL(hazelcastInstance.getMap("imap"), 100L, "check", TTLCleanMode.FUZZY_MATCHING);
		Assertions.assertTrue(resultPersistenceStorage == persistenceStorage);
	}

	@Test
	public void setImapTTLTest_ttlIsZero() throws Exception {
		PersistenceMongoDBConfig imapMongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap")
				.uri("mongodb://root:pwd@139.198.127.204:32550/qa?authSource=admin")
				.database("hazelcast")
				.collection("imap_default_config");
		persistenceStorage.addConfig(imapMongoDBConfig);
		PersistenceStorage resultPersistenceStorage = persistenceStorage.setImapTTL(hazelcastInstance.getMap("imap"), 0L, "check", TTLCleanMode.FUZZY_MATCHING);
		Assert.isNull(resultPersistenceStorage);
	}

	@Test
	public void setImapTTLTest_ttlIsNegativeNumber() throws Exception {
		PersistenceMongoDBConfig imapMongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap")
				.uri("mongodb://root:pwd@139.198.127.204:32550/qa?authSource=admin")
				.database("hazelcast")
				.collection("imap_default_config");
		persistenceStorage.addConfig(imapMongoDBConfig);
		PersistenceStorage resultPersistenceStorage = persistenceStorage.setImapTTL(hazelcastInstance.getMap("imap"), -2L, "check", TTLCleanMode.FUZZY_MATCHING);
		Assert.isNull(resultPersistenceStorage);
	}
}
