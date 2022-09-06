package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.persistence.ConfigConstant.MONGO_DB;
import static com.hazelcast.persistence.ConfigConstant.MONGO_URI;
import static com.hazelcast.persistence.ConfigConstant.ROCKSDB_DBPATH;

/**
 * @Author dayun
 * @Date 9/6/22
 */

public class ConfigTest {

    @Test
    public void testInitializeDefaultConfig() {
        Config c = new Config();
        PersistenceStorage ps = PersistenceStorage.getInstance();
        ps.initHZConfig(c);

        Config refConfig = ps.getConfig();
        MapConfig mapConfig = refConfig.getMapConfigOrNull("default");
        Assert.assertNotNull(mapConfig);

        final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        final String rocksDbPath = mapStoreConfig.getProperty(ROCKSDB_DBPATH);
        final String mongoUri = mapStoreConfig.getProperty(MONGO_URI);
        Assert.assertNotNull(rocksDbPath);
        Assert.assertNull(mongoUri);
        Assert.assertEquals("./imap-cache-data/", rocksDbPath);
        Assert.assertEquals(RocksDBIMap.class.getName(), mapStoreConfig.getClassName());
    }

    @Test
    public void testAddConfig() throws InterruptedException {
        Config c = new Config();
        PersistenceStorage ps = PersistenceStorage.getInstance();
        ps.initHZConfig(c);

        Thread t1 = new Thread(() -> {
            HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);
            ExternalStorageConfig config1 = new ExternalStorageConfig();
            config1.setConfigName("config1");
            config1.setStorageMode(StorageMode.MongoDB);
            MongoDBConfig mongoDBConfig = new MongoDBConfig();
            mongoDBConfig.setMongoUrl("mongodb://root:Gotapd8!@192.168.1.181");
            mongoDBConfig.setDb("xxx");
            mongoDBConfig.setCollection("testCollection");
            config1.setConfig(mongoDBConfig);
            ps.addConfig(config1);

            ExternalStorageConfig config2 = new ExternalStorageConfig();
            config2.setConfigName("config2");
            config2.setStorageMode(StorageMode.RocksDB);
            RocksDBConfig rocksDBConfig = new RocksDBConfig();
            rocksDBConfig.setDbPath("/xxx");
            config2.setConfig(rocksDBConfig);
            ps.addConfig(config2);
        });

        Thread t2 = new Thread(() -> {
            Map<String, MapConfig> configMap = c.getMapConfigs();
            Assert.assertEquals(3, configMap.size());

            MapConfig mapConfig2 = c.getMapConfigOrNull("config2");
            MapConfig mapConfig1 = c.getMapConfigOrNull("config1");
            Assert.assertNotNull(mapConfig1);
            Assert.assertNotNull(mapConfig2);

            MapStoreConfig mapStoreConfig1 = mapConfig1.getMapStoreConfig();
            Assert.assertEquals("xxx", mapStoreConfig1.getProperty(MONGO_DB));

            MapStoreConfig mapStoreConfig2 = mapConfig2.getMapStoreConfig();
            Assert.assertEquals("/xxx", mapStoreConfig2.getProperty("rocksdb.dbPath"));
        });

        t1.start();
        t1.join();
        t2.start();
        t2.join();
    }

    @Test
    public void testModifyConfig() throws InterruptedException {
        Config c = new Config();
        PersistenceStorage ps = PersistenceStorage.getInstance();
        ps.initHZConfig(c);

        Thread t1 = new Thread(() -> {
            HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

            ExternalStorageConfig config2 = new ExternalStorageConfig();
            config2.setConfigName("config2");
            config2.setStorageMode(StorageMode.RocksDB);
            RocksDBConfig rocksDBConfig = new RocksDBConfig();
            rocksDBConfig.setDbPath("/xxx");
            config2.setConfig(rocksDBConfig);
            ps.addConfig(config2);
        });

        Thread t2 = new Thread(() -> {
            MapConfig mapConfig2 = ps.getConfig().getMapConfigOrNull("config2");
            Assert.assertNotNull(mapConfig2);
            Assert.assertEquals("/xxx", mapConfig2.getMapStoreConfig().getProperty(ROCKSDB_DBPATH));

            ExternalStorageConfig config3 = new ExternalStorageConfig();
            config3.setConfigName("config2");
            config3.setStorageMode(StorageMode.RocksDB);
            RocksDBConfig rocksDBConfig = new RocksDBConfig();
            rocksDBConfig.setDbPath("/yyy");
            config3.setConfig(rocksDBConfig);
            ps.modifyConfig(config3);
        });

        Thread t3 = new Thread(() -> {
            MapConfig mapConfig2 = c.getMapConfigOrNull("config2");
            Assert.assertNotNull(mapConfig2);
            MapStoreConfig mapStoreConfig2 = mapConfig2.getMapStoreConfig();
            Assert.assertEquals("/yyy", mapStoreConfig2.getProperty(ROCKSDB_DBPATH));
        });

        t1.start();
        t1.join();
        t2.start();
        t2.join();
        t3.start();
        t3.join();
    }
}
