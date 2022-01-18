package com.hazelcast.persistence;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import com.hazelcast.map.IMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.bson.Document;


public class Application {
    public static void main(String[] args) throws InterruptedException {
        Config c = new Config();
        PersistenceStorage persistenceStorage = new PersistenceStorage();
        persistenceStorage
                .setStorageMode(StorageMode.MongoDB) // 设置存储引擎, 支持 MongoDB, RocksDB, Mem
                .setDB("cache") // 在存储引擎为 MongoDB 时有效, 设置数据库
                .setCollection("collection") // 在存储引擎为 MongoDB 时有效, 设置数据集合
                .setMongoUri("mongodb://127.0.0.1") // 在存储引擎为 MongoDB 时有效, 设置 mongodb uri
                .setInMemSize(1000) // 设置内存 buffer size
                .setRocksDBPath("./rocksdb-data/"); // 在存储引擎为 RocksDB 时有效, 设置 dbPath

        persistenceStorage.initHZConfig(c);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);

        String key = "xxx";
        Document value = new Document().append("x", 1).append("y", "sss");

        IMap<String, Document> m = hz.getMap("imapCache");
        System.out.println(m.get(key));
        m.put(key, value);
        System.out.println(m.get(key));

        Ringbuffer<Document> rb = hz.getRingbuffer("ringBufferCache");
        rb.add(value);
        System.out.println(rb.readOne(rb.tailSequence()));
    }
}
