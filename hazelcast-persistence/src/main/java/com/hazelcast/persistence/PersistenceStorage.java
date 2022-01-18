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

    public PersistenceStorage setStorageMode(StorageMode storageMode) {
        this.setImapStorageMode(storageMode);
        this.setRingBufferStorageMode(storageMode);
        return this;
    }

    public PersistenceStorage setImapStorageMode(StorageMode storageMode) {
        this.imapStorageMode = storageMode;
        return this;
    }

    public PersistenceStorage setRingBufferStorageMode(StorageMode storageMode) {
        this.ringBufferStorageMode = storageMode;
        return this;
    }

    public PersistenceStorage setRocksDBPath(String rocksDBPath) {
        this.setImapRocksDBPath(rocksDBPath);
        this.setRingBufferRocksDBPath(rocksDBPath);
        return this;
    }

    public PersistenceStorage setImapRocksDBPath(String rocksDBPath) {
        this.imapRocksDBPath = rocksDBPath;
        return this;
    }

    public PersistenceStorage setRingBufferRocksDBPath(String rocksDBPath) {
        this.ringBufferRocksDBPath = rocksDBPath;
        return this;
    }

    public PersistenceStorage setMongoUri(String mongoUri) {
        this.setImapMongoUri(mongoUri);
        this.setRingBufferMongoUri(mongoUri);
        return this;
    }

    public PersistenceStorage setImapMongoUri(String mongoUri) {
        this.imapMongoUri = mongoUri;
        return this;
    }

    public PersistenceStorage setRingBufferMongoUri(String mongoUri) {
        this.ringBufferMongoUri = mongoUri;
        return this;
    }

    public PersistenceStorage setDB(String db) {
        this.setImapDB(db);
        this.setRingBufferDB(db);
        return this;
    }

    public PersistenceStorage setImapDB(String db) {
        this.imapDB = db;
        return this;
    }

    public PersistenceStorage setRingBufferDB(String db) {
        this.ringBufferDB = db;
        return this;
    }

    public PersistenceStorage setCollection(String collection) {
        this.setImapCollection(collection);
        this.setRingBufferCollection((collection));
        return this;
    }

    public PersistenceStorage setImapCollection(String imapCollection) {
        this.imapCollection = imapCollection;
        return this;
    }

    public PersistenceStorage setRingBufferCollection(String ringBufferCollection) {
        this.ringBufferCollection = ringBufferCollection;
        return this;
    }

    public PersistenceStorage setInMemSize(Integer inMemSize) {
        this.setImapInMemSize(inMemSize);
        this.setRingBufferInMemSize(inMemSize);
        return this;
    }

    public PersistenceStorage setImapInMemSize(Integer imapInMemSize) {
        this.imapInMemSize = imapInMemSize;
        return this;
    }

    public PersistenceStorage setRingBufferInMemSize(Integer ringBufferInMemSize) {
        this.ringBufferInMemSize = ringBufferInMemSize;
        return this;
    }

    private PersistenceStorage initMapStoreConfig(Config c) {
        if (this.imapStorageMode == StorageMode.Mem) {
            return this;
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
        return this;
    }

    private PersistenceStorage initRingBufferConfig(Config c) {
        if (this.ringBufferStorageMode == StorageMode.Mem) {
            return this;
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
        return this;
    }


    public PersistenceStorage initHZConfig(Config c) {
        this.initMapStoreConfig(c);
        this.initRingBufferConfig(c);
        return this;
    }
}
