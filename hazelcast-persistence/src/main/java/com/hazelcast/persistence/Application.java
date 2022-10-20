package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.bson.Document;

import java.util.Random;


public class Application {
	public static void main(String[] args) throws InterruptedException {
		Config config = new Config();
		PersistenceStorage persistenceStorage = new PersistenceStorage();
		persistenceStorage
				.baseUrl("http://localhost:3000/api")
				.setStorageMode(StorageMode.HTTP_TM)
				.accessCode("3324cfdf-7d3e-4792-bd32-571638d4562f");
		persistenceStorage.initMapStoreConfig(config, "test");
		HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		IMap<Object, Object> test = hazelcastInstance.getMap("test");
		test.clear();
		test.put("_long", new Random().nextLong());
		test.put("_doc", new Document("name", "test1").append("age", 18));
		Object doc = test.get("_doc");
		System.out.println(doc);
		test.delete("_long");
		test.clear();
		hazelcastInstance.shutdown();
	}
}