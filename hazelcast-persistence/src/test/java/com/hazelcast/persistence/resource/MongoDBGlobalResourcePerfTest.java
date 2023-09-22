package com.hazelcast.persistence.resource;

import com.hazelcast.persistence.ConstructType;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.resource.impl.MongoDBGlobalResource;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * @author samuel
 * @Description
 * @create 2023-09-22 16:54
 **/
public class MongoDBGlobalResourcePerfTest {
	public static void main(String[] args) {
		testInsert(1);
	}

	public static void testInsert(int poolSize) {
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "map1")
				.uri("mongodb://localhost/27107")
				.database("test");
		ExecutorService executorService = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
		List<CompletableFuture<?>> list = new ArrayList<>();
		IntStream.range(0, poolSize).forEach(i -> list.add(CompletableFuture.runAsync(() -> {
			try {
				MongoClient mongoClient = MongoDBGlobalResource.getInstance().getMongoClient(mongoDBConfig);
				MongoDatabase database = mongoClient.getDatabase(mongoDBConfig.getDatabase());
				MongoCollection<Document> collection = database.getCollection("imap" + i);
				collection.drop();
//				try (
//						MongoCursor<Document> iterator = collection.find().iterator()
//				) {
//					while (iterator.hasNext()) {
//						Document doc = iterator.next();
//					}
//				}
				while (true) {
					List<WriteModel<Document>> writeModels = new ArrayList<>();
					IntStream.range(0, 2).forEach(i1 -> writeModels.add(new InsertOneModel<>(new Document("thread-name", Thread.currentThread().getName()).append("index", i1).append("now", new Date()))));
					collection.bulkWrite(writeModels);
					try {
						TimeUnit.MILLISECONDS.sleep(1000L);
					} catch (InterruptedException ignored) {
					}
				}
			} finally {
				MongoDBGlobalResource.getInstance().close(mongoDBConfig);
			}
		}, executorService)));
		System.out.println("All thread create finished");
		list.forEach(CompletableFuture::join);
	}
}
