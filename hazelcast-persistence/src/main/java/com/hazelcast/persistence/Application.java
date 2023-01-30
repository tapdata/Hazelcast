package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.bson.Document;


public class Application {
	public static void main(String[] args) throws Throwable {
		Document valueDoc = new Document("id", 1).append("name", "test1");
		Config config = new Config();
		PersistenceStorage.getInstance()
				.setStorageMode(StorageMode.MongoDB)
				.setMongoUri("mongodb://localhost/")
				.setDB("test")
				.setCollection("cache1")
				.initMapStoreConfig(config);
		HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		IMap<Object, Object> map1 = hazelcastInstance.getMap("map1");
		map1.put("value", valueDoc);
		PersistenceStorage.getInstance()
				.setDB("test").setCollection("imap")
				.initMapStoreConfig(config, "map2");
		IMap<Object, Object> map2 = hazelcastInstance.getMap("map2");
		map2.clear();
		map2.put("value", valueDoc);
		PersistenceStorage.getInstance()
				.setDB("test").setCollection("ringbuffer")
				.initRingBufferConfig(config, "ring2");
		Ringbuffer<Object> ring2 = hazelcastInstance.getRingbuffer("ring2");
		ring2.destroy();
		ring2.add(valueDoc);
		hazelcastInstance.shutdown();
	}
}