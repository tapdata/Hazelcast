package com.hazelcast.persistence;

import com.hazelcast.config.*;

public class PersistenceStorage {
    private StorageMode imapStorageMode = StorageMode.RocksDB;
    private String imapRocksDBPath = "./imap-cache-data/";
    private String imapMongoUri = "mongodb://127.0.0.1";
    private String imapDB = "cache";
    private String imapCollection = "imap";
    private Integer imapInMemSize = 1;

    private StorageMode ringBufferStorageMode = StorageMode.RocksDB;
    private String ringBufferRocksDBPath = "./ringBuffer-cache-data/";
    private String ringBufferMongoUri = "mongodb://127.0.0.1";
    private String ringBufferDB = "cache";
    private String ringBufferCollection = "ringBuffer";
    private Integer ringBufferInMemSize = 1;

    public void setStorageMode(StorageMode storageMode) {
        this.setImapStorageMode(storageMode);
        this.setRingBufferStorageMode(storageMode);
    }

    public void setImapStorageMode(StorageMode storageMode) {
        this.imapStorageMode = storageMode;
    }

    public void setRingBufferStorageMode(StorageMode storageMode) {
        this.ringBufferStorageMode = storageMode;
    }

    public void setRocksDBPath(String rocksDBPath) {
        this.setImapRocksDBPath(rocksDBPath);
        this.setRingBufferRocksDBPath(rocksDBPath);
    }

    public void setImapRocksDBPath(String rocksDBPath) {
        this.imapRocksDBPath = rocksDBPath;
    }

    public void setRingBufferRocksDBPath(String rocksDBPath) {
        this.ringBufferRocksDBPath = rocksDBPath;
    }

    public void setMongoUri(String mongoUri) {
        this.setImapMongoUri(mongoUri);
        this.setRingBufferMongoUri(mongoUri);
    }

    public void setImapMongoUri(String mongoUri) {
        this.imapMongoUri = mongoUri;
    }

    public void setRingBufferMongoUri(String mongoUri) {
        this.ringBufferMongoUri = mongoUri;
    }

    public void setDB(String db) {
        this.setImapDB(db);
        this.setRingBufferDB(db);
    }

    public void setImapDB(String db) {
        this.imapDB = db;
    }

    public void setRingBufferDB(String db) {
        this.ringBufferDB = db;
    }

    public void setCollection(String collection) {
        this.setImapCollection(collection);
        this.setRingBufferCollection((collection));
    }

    public void setImapCollection(String imapCollection) {
        this.imapCollection = imapCollection;
    }

    public void setRingBufferCollection(String ringBufferCollection) {
        this.ringBufferCollection = ringBufferCollection;
    }

    public void setInMemSize(Integer inMemSize) {
        this.setImapInMemSize(inMemSize);
        this.setRingBufferInMemSize(inMemSize);
    }

    public void setImapInMemSize(Integer imapInMemSize) {
        this.imapInMemSize = imapInMemSize;
    }

    public void setRingBufferInMemSize(Integer ringBufferInMemSize) {
        this.ringBufferInMemSize = ringBufferInMemSize;
    }

    private void initMapStoreConfig(Config c) {
        if (this.imapStorageMode == StorageMode.Mem) {
            return;
        }
        MapConfig mapCfg = new MapConfig();
        mapCfg.setName("default");
        MapStoreConfig mapStoreCfg = new MapStoreConfig();
        switch (this.imapStorageMode) {
            case MongoDB:
                mapStoreCfg.setClassName(MonogoDBIMap.class.getName())
                        .setProperty("mongo.uri", this.imapMongoUri)
                        .setProperty("mongo.db", this.imapDB)
                        .setProperty("mongo.collection", this.imapCollection);
                break;
            case RocksDB:
                mapStoreCfg.setClassName(RocksdbIMap.class.getName())
                        .setProperty("rocksdb.dbPath", this.imapRocksDBPath);
        }
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(this.imapInMemSize);
        mapCfg.setEvictionConfig(evictionConfig);
        mapStoreCfg.setEnabled(true);
        mapCfg.setMapStoreConfig(mapStoreCfg);
        c.addMapConfig(mapCfg);
    }

    private void initRingBufferConfig(Config c) {
        if (this.ringBufferStorageMode == StorageMode.Mem) {
            return;
        }

        RingbufferConfig ringbufferConfig = new RingbufferConfig();
        ringbufferConfig.setName("default");
        RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig();
        switch (this.ringBufferStorageMode) {
            case MongoDB:
                ringbufferStoreConfig.setClassName(MongoDBRingBuffer.class.getName())
                        .setProperty("mongo.uri", this.ringBufferMongoUri)
                        .setProperty("mongo.db", this.ringBufferDB)
                        .setProperty("mongo.collection", this.ringBufferCollection);
                break;
            case RocksDB:
                ringbufferStoreConfig.setClassName(RocksDBRingBuffer.class.getName())
                        .setProperty("rocksdb.dbPath", this.ringBufferRocksDBPath);
        }
        ringbufferConfig.setCapacity(this.ringBufferInMemSize).setInMemoryFormat(InMemoryFormat.OBJECT);
        ringbufferStoreConfig.setEnabled(true);
        ringbufferConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
        c.addRingBufferConfig(ringbufferConfig);
    }


    public void initHZConfig(Config c) {
        this.initMapStoreConfig(c);
        this.initRingBufferConfig(c);
    }
}
