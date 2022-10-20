package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.bson.Document;

import java.util.Random;


public class Application {
	public static void main(String[] args) throws Throwable {
		Config config = new Config();
		PersistenceStorage persistenceStorage = new PersistenceStorage();
		persistenceStorage
				.setStorageMode(StorageMode.HTTP_TM)
				.baseUrl("http://localhost:3000/api")
				.accessCode("3324cfdf-7d3e-4792-bd32-571638d4562f");
		try {
			persistenceStorage.initMapStoreConfig(config, "pdkStateMap-1");
		} catch (Exception e) {
			e.printStackTrace();
		}
		HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		IMap<Object, Object> test = hazelcastInstance.getMap("pdkStateMap-1");
		try {
			test.clear();
			test.put("_long", new Random().nextLong());
			test.put("_doc", new Document("name", "test1").append("age", 18));
			Object doc = test.get("_doc");
			System.out.println(doc);
			test.delete("_long");
		} catch (Throwable e) {
			e.printStackTrace();
		}
		new PersistenceStorage()
				.setStorageMode(StorageMode.HTTP_TM)
				.baseUrl("http://localhost:8080/api")
				.accessCode("3324cfdf-7d3e-4792-bd32-571638d4562f")
				.initMapStoreConfig(hazelcastInstance.getConfig(), "pdkStateMap-2");
		IMap<Object, Object> stateMap2 = hazelcastInstance.getMap("pdkStateMap-2");
		try {
			stateMap2.clear();
			stateMap2.put("test", new Document("id", 1).append("name", "test1"));
		} catch (Throwable e) {
			e.printStackTrace();
		}
		hazelcastInstance.shutdown();
	}
}