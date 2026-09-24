package com.hazelcast.persistence.store.impl;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class MongoDBIMapTest {
	@Test
	void mongoSixStatisticsShouldSumShardValues() {
		Document result = MongoDBIMap.aggregateCollectionStats(Arrays.asList(
				new Document("storageStats", new Document("count", 2L).append("storageSize", 20L)),
				new Document("storageStats", new Document("count", 3L).append("storageSize", 30L))));

		Assertions.assertEquals(5L, result.get("count"));
		Assertions.assertEquals(50L, result.get("storageSize"));
	}
}
