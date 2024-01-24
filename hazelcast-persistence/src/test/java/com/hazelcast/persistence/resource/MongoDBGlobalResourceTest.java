package com.hazelcast.persistence.resource;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBGlobalResource;
import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-09-19 16:52
 **/
class MongoDBGlobalResourceTest {

	@Test
	void testGetInstance() {
		MongoDBGlobalResource instance1 = MongoDBGlobalResource.getInstance();
		MongoDBGlobalResource instance2 = MongoDBGlobalResource.getInstance();
		Assertions.assertSame(instance1, instance2);
	}

	@Test
	void testGetOneMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		MongoClient mongoClient = instance.getMongoClient(mongoDBConfig);
		Assertions.assertNotNull(mongoClient);
		instance.close(mongoDBConfig);
	}

	@Test
	void testGetTwoSameMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig1 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		PersistenceMongoDBConfig mongoDBConfig2 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		MongoClient mongoClient1 = instance.getMongoClient(mongoDBConfig1);
		MongoClient mongoClient2 = instance.getMongoClient(mongoDBConfig2);
		Assertions.assertSame(mongoClient1, mongoClient2);
	}

	@Test
	void testGetTwoDiffNameMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig1 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		PersistenceMongoDBConfig mongoDBConfig2 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map2")
				.uri("mongodb://user:pwd@localhost:27017");
		MongoClient mongoClient1 = instance.getMongoClient(mongoDBConfig1);
		MongoClient mongoClient2 = instance.getMongoClient(mongoDBConfig2);
		Assertions.assertNotSame(mongoClient1, mongoClient2);
		instance.close(mongoDBConfig1);
		instance.close(mongoDBConfig2);
	}

	@Test
	void testGetTwoDiffUriMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig1 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		PersistenceMongoDBConfig mongoDBConfig2 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27018");
		MongoClient mongoClient1 = instance.getMongoClient(mongoDBConfig1);
		MongoClient mongoClient2 = instance.getMongoClient(mongoDBConfig2);
		Assertions.assertNotSame(mongoClient1, mongoClient2);
		instance.close(mongoDBConfig1);
		instance.close(mongoDBConfig2);
	}

	@Test
	void testNullConfig() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		Assertions.assertThrows(IllegalArgumentException.class, () -> instance.getMongoClient(null));
	}

	@Test
	void testInvalidMongoUri() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongo://user:pwd@localhost:27017");
		Assertions.assertThrows(IllegalArgumentException.class, () -> instance.getMongoClient(mongoDBConfig));
	}

	@Test
	void testCreateAndCloseMongoClient() throws Exception {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		instance.getMongoClient(mongoDBConfig);
		instance.close(mongoDBConfig);
		Field resourceMapField = instance.getClass().getDeclaredField("RESOURCE_MAP");
		resourceMapField.setAccessible(true);
		Object resourceMap = resourceMapField.get(instance);
		Assertions.assertTrue(resourceMap instanceof Map);
		Assertions.assertTrue(((Map) resourceMap).isEmpty());
	}

	@Test
	void testCreateTwiceAndCloseOnceMongoClient() throws Exception {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		instance.getMongoClient(mongoDBConfig);
		instance.getMongoClient(mongoDBConfig);
		instance.close(mongoDBConfig);
		Field resourceMapField = instance.getClass().getDeclaredField("RESOURCE_MAP");
		resourceMapField.setAccessible(true);
		Object resourceMap = resourceMapField.get(instance);
		Assertions.assertTrue(resourceMap instanceof Map);
		Assertions.assertEquals(1, ((Map) resourceMap).size());
		instance.close(mongoDBConfig);
	}

	@Test
	void testNullConfigCloseMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		Assertions.assertDoesNotThrow(() -> instance.close(null));
	}
}
