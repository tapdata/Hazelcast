package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.utils.SSLUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author samuel
 * @Description
 * @create 2023-09-19 16:49
 **/
public class MongoDBGlobalResource {
	private final static Map<String, MongoClientPartition> RESOURCE_MAP = new ConcurrentHashMap<>();
	public static final String MONGODB_MAX_WAIT_QUEUE_SIZE = "mongodb_maxWaitQueueSize";
	public static final int DEFAULT_MONGODB_MAX_WAIT_QUEUE_SIZE = 100000;
	public static final String MONGODB_MAX_SIZE = "mongodb_maxSize";
	public static final int DEFAULT_MONGODB_MAX_SIZE = 100;

	private MongoDBGlobalResource() {
	}

	public static MongoDBGlobalResource getInstance() {
		return SingleTon.INSTANCE.getInstance();
	}

	public MongoClient getMongoClient(PersistenceMongoDBConfig persistenceMongoDBConfig) {
		if (null == persistenceMongoDBConfig) {
			throw new IllegalArgumentException("MongoDB config cannot be null");
		}
		if (StringUtils.isBlank(persistenceMongoDBConfig.getUri())) {
			throw new IllegalArgumentException("MongoDB uri can not be null");
		}
		String mongoClientKey = getMongoClientKey(persistenceMongoDBConfig.getUri());
		return RESOURCE_MAP.computeIfAbsent(mongoClientKey, key -> new MongoClientPartition()).getMongoClientWithPartition(persistenceMongoDBConfig);
	}

	public void close(PersistenceMongoDBConfig persistenceMongoDBConfig) {
		if (null == persistenceMongoDBConfig) {
			return;
		}
		String mongoClientKey = getMongoClientKey(persistenceMongoDBConfig.getUri());
		RESOURCE_MAP.computeIfPresent(mongoClientKey, (key, value) -> {
			if (value.close(persistenceMongoDBConfig)) {
				RESOURCE_MAP.remove(mongoClientKey);
			}
			return value;
		});
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

	private static class MongoClientPartition {
		public static final int DEFAULT_PARTITION_SIZE = 8;
		private Map<String, MongoClientHolder> mongoClientHolderMap = new ConcurrentHashMap<>();
		private int partitionSize = DEFAULT_PARTITION_SIZE;

		public MongoClientPartition partitionSize(int partitionSize) {
			this.partitionSize = partitionSize;
			return this;
		}

		public int getPartitionSize() {
			return partitionSize;
		}

		public MongoClient getMongoClientWithPartition(PersistenceMongoDBConfig persistenceMongoDBConfig) {
			int partitionCode = getPartitionCode(persistenceMongoDBConfig);
			return mongoClientHolderMap.computeIfAbsent(partitionCode + "", key -> new MongoClientHolder(persistenceMongoDBConfig, this)).getMongoClient();
		}

		private int getPartitionCode(PersistenceMongoDBConfig persistenceMongoDBConfig) {
			String name = persistenceMongoDBConfig.getName();
			int hash = Objects.hash(name);
			return hash % partitionSize;
		}

		public boolean close(PersistenceMongoDBConfig persistenceMongoDBConfig) {
			int partitionCode = getPartitionCode(persistenceMongoDBConfig);
			return mongoClientHolderMap.computeIfPresent(partitionCode + "", (k, v) -> {
				if (v.close()) {
					return null;
				}
				return v;
			}) == null;
		}
	}

	private static class MongoClientHolder {
		private final AtomicInteger usage = new AtomicInteger(0);
		private final PersistenceMongoDBConfig persistenceMongoDBConfig;
		private MongoClient mongoClient;
		private MongoClientPartition mongoClientPartition;

		public MongoClientHolder(PersistenceMongoDBConfig persistenceMongoDBConfig) {
			this.persistenceMongoDBConfig = persistenceMongoDBConfig;
		}

		public MongoClientHolder(PersistenceMongoDBConfig persistenceMongoDBConfig, MongoClientPartition mongoClientPartition) {
			this.persistenceMongoDBConfig = persistenceMongoDBConfig;
			this.mongoClientPartition = mongoClientPartition;
		}

		public synchronized MongoClient getMongoClient() {
			if (null == mongoClient) {
				String uri = persistenceMongoDBConfig.getUri();
				MongoClientSettings.Builder mongoClientSettingBuilder = MongoClientSettings.builder();
				setSSLSettingIfNeed(mongoClientSettingBuilder);
				mongoClientSettingBuilder.applyToConnectionPoolSettings(connectionPoolSettings -> {
					int maxWaitQueueSize = CommonUtils.getPropertyInt(MONGODB_MAX_WAIT_QUEUE_SIZE, DEFAULT_MONGODB_MAX_WAIT_QUEUE_SIZE);
					int maxSize = CommonUtils.getPropertyInt(MONGODB_MAX_SIZE, DEFAULT_MONGODB_MAX_SIZE);
					connectionPoolSettings.maxWaitQueueSize(maxWaitQueueSize)
							.maxSize(maxSize);
				});
				mongoClient = MongodbUtil.createClient(uri, mongoClientSettingBuilder.build());
			}
			usage.incrementAndGet();
			return mongoClient;
		}

		private void setSSLSettingIfNeed(MongoClientSettings.Builder mongoClientSettingBuilder) {
			try {
				if (persistenceMongoDBConfig.isSsl()) {
					String uri = persistenceMongoDBConfig.getUri();
					if (uri.indexOf("tlsAllowInvalidCertificates=true") > 0 ||
							uri.indexOf("sslAllowInvalidCertificates=true") > 0) {
						mongoClientSettingBuilder.applyToSslSettings(ssl -> {
							SSLContext sslContext;
							try {
								sslContext = SSLContext.getInstance("SSL");
								sslContext.init(null, new TrustManager[]{new X509TrustManager() {
									@Override
									public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
									}

									@Override
									public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
									}

									@Override
									public X509Certificate[] getAcceptedIssuers() {
										return null;
									}
								}}, new SecureRandom());
							} catch (Exception e) {
								throw new RuntimeException(String.format("Init SSL context failed, error: %s", e.getMessage()), e);
							}
							ssl.enabled(true).context(sslContext).invalidHostNameAllowed(true);
						});
					} else {
						List<String> trustCertificates = null;
						if (persistenceMongoDBConfig.isSslValidate()) {
							trustCertificates = SSLUtil.retriveCertificates(persistenceMongoDBConfig.getSslCA());
						}
						List<String> clientCertificates = SSLUtil.retriveCertificates(persistenceMongoDBConfig.getSslKey());
						String clientPrivateKey = SSLUtil.retrivePrivateKey(persistenceMongoDBConfig.getSslKey());
						if (StringUtils.isNotBlank(clientPrivateKey) && CollectionUtils.isNotEmpty(clientCertificates)) {
							SSLContext sslContext = SSLUtil.createSSLContext(clientPrivateKey, clientCertificates, trustCertificates, persistenceMongoDBConfig.getSslPass());
							mongoClientSettingBuilder.applyToSslSettings(ssl -> ssl.enabled(true).context(sslContext).invalidHostNameAllowed(!persistenceMongoDBConfig.isCheckServerIdentity()));
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException(String.format("MongoDB set client ssl setting failed: %s, config: %s", e.getMessage(), persistenceMongoDBConfig), e);
			}
		}

		public synchronized boolean close() {
			if (usage.decrementAndGet() <= 0) {
				if (null != mongoClient) {
					mongoClient.close();
					mongoClient = null;
					return true;
				}
			}
			return false;
		}
	}
}
