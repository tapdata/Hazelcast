package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-04-28 16:28
 **/
public class LoadMapDataTest {
	private static HazelcastInstance hazelcastInstance;
	private static PersistenceStorage persistenceStorage;

	public static void main(String[] args) throws Exception {
		init();
//		genData();
		getData();
	}

	public static void init() {
		Config config = new Config();
		config.getJetConfig().setEnabled(true);
		JoinConfig joinConfig = new JoinConfig();
		joinConfig.setTcpIpConfig(new TcpIpConfig().setEnabled(true));
		NetworkConfig networkConfig = new NetworkConfig();
		networkConfig.setJoin(joinConfig);
		config.setNetworkConfig(networkConfig);
		config.setInstanceName("test");
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		persistenceStorage = PersistenceStorage.getInstance();
	}

	public static void genData() throws Exception {
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap")
				.uri("mongodb://localhost/")
				.database("hazelcast")
				.collection("test");
		persistenceStorage.addConfig(mongoDBConfig).initMapStoreConfig(hazelcastInstance.getConfig(), mongoDBConfig.getName());
		IMap<String, Document> map = hazelcastInstance.getMap(mongoDBConfig.getName());
		persistenceStorage.clear(ConstructType.IMAP, mongoDBConfig.getName());
		// 生成10000条用户数据，写入map
		int num = 5000000;
		Map<String, Document> cache = new HashMap<>();
		for (int i = 0; i < num; i++) {
			Document document = new Document("id", (i + 1))
					.append("name", RandomStringUtils.randomAlphabetic(10))
					.append("status", i % 2 == 0)
					.append("create_dte", new Date());
			cache.put(String.valueOf(i + 1), document);
			if (cache.size() % 1000 == 0) {
				map.putAll(cache);
				cache.clear();
			}
		}
		if (MapUtils.isNotEmpty(cache)) {
			map.putAll(cache);
			cache.clear();
		}
	}

	public static void getData() throws Exception {
		PersistenceMongoDBConfig mongoDBConfig = PersistenceMongoDBConfig.create(ConstructType.IMAP, "imap")
				.uri("mongodb://localhost/")
				.database("hazelcast")
				.collection("test");
		persistenceStorage.addConfig(mongoDBConfig).initMapStoreConfig(hazelcastInstance.getConfig(), mongoDBConfig.getName());
		IMap<String, Document> map = hazelcastInstance.getMap(mongoDBConfig.getName());
		int i = RandomUtils.nextInt(10000, 20000);
		Document document = map.get(String.valueOf(i));
		System.out.println(document.toJson(JsonWriterSettings.builder().indent(true).build()));
	}
}
