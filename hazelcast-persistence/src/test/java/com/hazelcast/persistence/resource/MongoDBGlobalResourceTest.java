package com.hazelcast.persistence.resource;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBGlobalResource;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.lang3.RandomUtils;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author samuel
 * @Description
 * @create 2023-09-19 16:52
 **/
public class MongoDBGlobalResourceTest {

	@Test
	public void testGetInstance() {
		MongoDBGlobalResource instance1 = MongoDBGlobalResource.getInstance();
		MongoDBGlobalResource instance2 = MongoDBGlobalResource.getInstance();
		Assertions.assertSame(instance1, instance2);
	}

	@Test
	public void testGetOneMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		MongoClient mongoClient = instance.getMongoClient(mongoDBConfig);
		Assertions.assertNotNull(mongoClient);
		instance.close(mongoDBConfig.getUri());
	}

	@Test
	public void testGetTwoSamMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig1 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		PersistenceMongoDBConfig mongoDBConfig2 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map2")
				.uri("mongodb://user:pwd@localhost:27017");
		MongoClient mongoClient1 = instance.getMongoClient(mongoDBConfig1);
		MongoClient mongoClient2 = instance.getMongoClient(mongoDBConfig2);
		Assertions.assertSame(mongoClient1, mongoClient2);
		instance.close(mongoDBConfig1.getUri());
		instance.close(mongoDBConfig2.getUri());
	}

	@Test
	public void testGetTwoDiffMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig1 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		PersistenceMongoDBConfig mongoDBConfig2 = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map2")
				.uri("mongodb://user:pwd@localhost:27018");
		MongoClient mongoClient1 = instance.getMongoClient(mongoDBConfig1);
		MongoClient mongoClient2 = instance.getMongoClient(mongoDBConfig2);
		Assertions.assertNotSame(mongoClient1, mongoClient2);
		instance.close(mongoDBConfig1.getUri());
		instance.close(mongoDBConfig2.getUri());
	}

	@Test
	public void testNullConfig() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		Assertions.assertThrows(IllegalArgumentException.class, () -> instance.getMongoClient(null));
	}

	@Test
	public void testInvalidMongoUri() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongo://user:pwd@localhost:27017");
		Assertions.assertThrows(IllegalArgumentException.class, () -> instance.getMongoClient(mongoDBConfig));
	}

	@Test
	public void testCreateAndCloseMongoClient() throws Exception {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		instance.getMongoClient(mongoDBConfig);
		instance.close(mongoDBConfig.getUri());
		Field resourceMapField = instance.getClass().getDeclaredField("RESOURCE_MAP");
		resourceMapField.setAccessible(true);
		Object resourceMap = resourceMapField.get(instance);
		Assertions.assertTrue(resourceMap instanceof Map);
		Assertions.assertTrue(((Map) resourceMap).isEmpty());
	}

	@Test
	public void testCreateTwiceAndCloseOnceMongoClient() throws Exception {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://user:pwd@localhost:27017");
		instance.getMongoClient(mongoDBConfig);
		instance.getMongoClient(mongoDBConfig);
		instance.close(mongoDBConfig.getUri());
		Field resourceMapField = instance.getClass().getDeclaredField("RESOURCE_MAP");
		resourceMapField.setAccessible(true);
		Object resourceMap = resourceMapField.get(instance);
		Assertions.assertTrue(resourceMap instanceof Map);
		Assertions.assertEquals(1, ((Map) resourceMap).size());
		instance.close(mongoDBConfig.getUri());
	}

	@Test
	public void testNullConfigCloseMongoClient() {
		MongoDBGlobalResource instance = MongoDBGlobalResource.getInstance();
		Assertions.assertDoesNotThrow(() -> instance.close(null));
	}

	/*@Test
	public void testMongoUsage() {
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://localhost/27107")
				.database("test");
		int poolSize = 500;
		ExecutorService executorService = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
		List<CompletableFuture<?>> list = new ArrayList<>();
		IntStream.range(0, poolSize).forEach(i -> list.add(CompletableFuture.runAsync(() -> {
			try {
				MongoClient mongoClient = MongoDBGlobalResource.getInstance().getMongoClient(mongoDBConfig);
				MongoDatabase database = mongoClient.getDatabase(mongoDBConfig.getDatabase());
				MongoCollection<Document> collection = database.getCollection("imap" + i);
				collection.drop();
				*//*try (
						MongoCursor<Document> iterator = collection.find().iterator()
				) {
					while (iterator.hasNext()) {
						Document doc = iterator.next();
					}
				}*//*
				while (true) {
					List<WriteModel<Document>> writeModels = new ArrayList<>();
					IntStream.range(0,10).forEach(i1-> writeModels.add(new InsertOneModel<>(new Document("thread-name", Thread.currentThread().getName()).append("index", i1).append("now", new Date()))));
					collection.bulkWrite(writeModels);
					try {
						TimeUnit.MILLISECONDS.sleep(1000L);
					} catch (InterruptedException ignored) {
					}
				}
			} finally {
				MongoDBGlobalResource.getInstance().close(mongoDBConfig.getUri());
			}
		}, executorService)));
		list.forEach(CompletableFuture::join);
	}

	@Test
	public void test() {
		MongoClientSettings.Builder builder = MongoClientSettings.builder();
		builder.applyToConnectionPoolSettings(poolSetting -> {
			poolSetting.maxWaitQueueSize(5000).maxSize(100);
		});
		builder.applyConnectionString(new ConnectionString("mongodb://localhost/test"));
		try (MongoClient mongoClient = MongoClients.create(builder.build())) {
			int poolSize = 2000;
			AtomicInteger errorCount = new AtomicInteger();
			ExecutorService executorService = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
			List<CompletableFuture<?>> completableFutures = new ArrayList<>();
			IntStream.range(0, poolSize).forEach(i -> completableFutures.add(CompletableFuture.runAsync(() -> {
				try {
					TimeUnit.MILLISECONDS.sleep(RandomUtils.nextLong(1L, 50L));
				} catch (InterruptedException ignored) {
				}
				MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("imap1");
				try (
						MongoCursor<Document> iterator = collection.find().iterator()
				) {
					while (iterator.hasNext()) {
						iterator.next();
					}
				} catch (Exception e) {
					System.err.println(Thread.currentThread().getName() + ": " + e.getMessage());
					errorCount.incrementAndGet();
				}
			}, executorService)));
			completableFutures.forEach(CompletableFuture::join);
			System.out.println("Error count: " + errorCount);
		}
	}*/
}
