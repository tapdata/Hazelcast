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
        persistenceStorage.setStorageMode(StorageMode.MongoDB);
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
