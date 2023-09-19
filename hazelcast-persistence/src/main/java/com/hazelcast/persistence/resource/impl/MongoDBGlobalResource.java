package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.utils.SSLUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.SSLContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author samuel
 * @Description
 * @create 2023-09-19 16:49
 **/
public class MongoDBGlobalResource {
	private final static Map<String, MongoClientHolder> RESOURCE_MAP = new ConcurrentHashMap<>();

	private MongoDBGlobalResource() {
	}

	public static MongoDBGlobalResource getInstance() {
		return SingleTon.INSTANCE.getInstance();
	}

	public MongoClient getMongoClient(PersistenceMongoDBConfig persistenceMongoDBConfig) {
		if (null == persistenceMongoDBConfig) {
			throw new IllegalArgumentException("Mongo config cannot be null");
		}
		if (StringUtils.isBlank(persistenceMongoDBConfig.getUri())) {
			throw new IllegalArgumentException("MongoDB uri can not be null");
		}
		String mongoClientKey = getMongoClientKey(persistenceMongoDBConfig.getUri());
		MongoClientHolder mongoClientHolder = RESOURCE_MAP.computeIfAbsent(mongoClientKey, key -> new MongoClientHolder(persistenceMongoDBConfig));
		return mongoClientHolder.getMongoClient();
	}

	public void close(String mongodbUri) {
		if (StringUtils.isBlank(mongodbUri)) {
			return;
		}
		String mongoClientKey = getMongoClientKey(mongodbUri);
		MongoClientHolder mongoClientHolder = RESOURCE_MAP.get(mongoClientKey);
		if (null != mongoClientHolder && mongoClientHolder.close()) {
			RESOURCE_MAP.remove(mongoClientKey);
		}
	}

	private static String getMongoClientKey(String mongodbUri) {
		ConnectionString connectionString = new ConnectionString(mongodbUri);
		String username = connectionString.getUsername();
		List<String> hosts = connectionString.getHosts();
		String hostStr = String.join(",", hosts);
		return username + "@" + hostStr;
	}

	private enum SingleTon {
		INSTANCE;

		private final static MongoDBGlobalResource mongoDBGlobalResource = new MongoDBGlobalResource();

		SingleTon() {
		}

		public MongoDBGlobalResource getInstance() {
			return mongoDBGlobalResource;
		}
	}

	private static class MongoClientHolder {
		private final AtomicInteger usage = new AtomicInteger(0);
		private final PersistenceMongoDBConfig persistenceMongoDBConfig;
		private MongoClient mongoClient;

		public MongoClientHolder(PersistenceMongoDBConfig persistenceMongoDBConfig) {
			this.persistenceMongoDBConfig = persistenceMongoDBConfig;
		}

		public synchronized MongoClient getMongoClient() {
			if (null == mongoClient) {
				String uri = persistenceMongoDBConfig.getUri();
				MongoClientOptions.Builder builder = MongoClientOptions.builder();
				try {
					List<String> trustCertificates = null;
					if (persistenceMongoDBConfig.isSslValidate()) {
						trustCertificates = SSLUtil.retriveCertificates(persistenceMongoDBConfig.getSslCA());
					}
					List<String> clientCertificates = SSLUtil.retriveCertificates(persistenceMongoDBConfig.getSslKey());
					String clientPrivateKey = SSLUtil.retrivePrivateKey(persistenceMongoDBConfig.getSslKey());
					if (StringUtils.isNotBlank(clientPrivateKey) && CollectionUtils.isNotEmpty(clientCertificates)) {
						SSLContext sslContext = SSLUtil.createSSLContext(clientPrivateKey, clientCertificates, trustCertificates, persistenceMongoDBConfig.getSslPass());
						builder.sslContext(sslContext);
						builder.sslEnabled(true);
						builder.sslInvalidHostNameAllowed(!persistenceMongoDBConfig.isCheckServerIdentity());
					}
				} catch (Exception e) {
					throw new RuntimeException(String.format("MongoDB set client ssl options failed: %s, config: %s", e.getMessage(), persistenceMongoDBConfig), e);
				}
				mongoClient = MongodbUtil.createClient(uri, builder.build());
			}
			usage.incrementAndGet();
			return mongoClient;
		}

		public synchronized boolean close() {
			if (usage.decrementAndGet() <= 0) {
				if (null != mongoClient) {
					mongoClient.close();
					return true;
				}
			}
			return false;
		}
	}
}
