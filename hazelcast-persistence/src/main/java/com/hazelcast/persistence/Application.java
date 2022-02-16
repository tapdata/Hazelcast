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
        PersistenceStorage persistenceStorage = PersistenceStorage.getInstance();
        persistenceStorage
                .setStorageMode(StorageMode.MongoDB) // 设置存储引擎, 支持 MongoDB, RocksDB, Mem
                .setDB("cache") // 在存储引擎为 MongoDB 时有效, 设置数据库
                .setCollection("collection") // 在存储引擎为 MongoDB 时有效, 设置数据集合
                .setMongoUri("mongodb://127.0.0.1") // 在存储引擎为 MongoDB 时有效, 设置 mongodb uri
                .setInMemSize(1) // 设置内存 buffer size
                .setRocksDBPath("./rocksdb-data/"); // 在存储引擎为 RocksDB 时有效, 设置 dbPath

        persistenceStorage.initHZConfig(c);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);
        String key = "xxx";
        Document value = new Document().append("x", 1).append("y", "sss").append("timestamp", 10000L);
        Document value1 = new Document().append("x", 1).append("y", "sss").append("timestamp", 10001L);
        Document value2 = new Document().append("x", 1).append("y", "sss").append("timestamp", 10002L);
        Document value3 = new Document().append("x", 1).append("y", "sss").append("timestamp", 10003L);
        Document value4 = new Document().append("x", 1).append("y", "sss").append("timestamp", 10004L);
        Document value5 = new Document().append("x", 1).append("y", "sss").append("timestamp", 10005L);

        IMap<String, Document> m = hz.getMap("imapCache");
        System.out.println(m.get(key));
        m.put(key, value);
        for (int i=0; i< 12; i++) {
            m.put(key + i, value);
        }
        System.out.println(m.get(key+5000));

        Ringbuffer<Document> rb = hz.getRingbuffer("ringBufferCache");
        System.out.println(rb.headSequence());
        System.out.println(rb.tailSequence());

        rb.add(value);
        rb.add(value1);
        rb.add(value2);
        rb.add(value3);
        rb.add(value4);
        rb.add(value5);
        System.out.println(rb.headSequence());
        System.out.println(rb.tailSequence());
        System.out.println(rb.readOne(0));
        System.out.println(rb.readOne(5));

        System.out.println(persistenceStorage.findSequence(rb, 9999));
        System.out.println(persistenceStorage.findSequence(rb, 10000));
        System.out.println(persistenceStorage.findSequence(rb, 10001));
        System.out.println(persistenceStorage.findSequence(rb, 10003));
        System.out.println(persistenceStorage.findSequence(rb, 10008));

//        System.out.println(rb.tailSequence());
//        System.out.println(rb.readOne(rb.tailSequence()));
//
//        for (int i=0; i < 10; i++) {
//            rb.add(value);
//        }
//        System.out.println(rb.size());
//        System.out.println(rb.capacity());
//        System.out.println(rb.remainingCapacity());
//        System.out.println(rb.headSequence());
//        System.out.println(rb.tailSequence());
//        System.out.println(rb.readOne(rb.tailSequence()));
    }
}