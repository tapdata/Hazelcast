package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.utils.SSLUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.core.api.PDKIntegration;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author samuel
 * @Description
 * @create 2023-09-19 16:49
 **/
public class MongoDBGlobalResource implements MemoryFetcher {
	private final static Map<String, MongoClientPartition> RESOURCE_MAP = new ConcurrentHashMap<>();
	public static final String MONGODB_MAX_WAIT_QUEUE_SIZE = "mongodb_maxWaitQueueSize";
	public static final int DEFAULT_MONGODB_MAX_WAIT_QUEUE_SIZE = 100000;
	public static final String MONGODB_MAX_SIZE = "mongodb_maxSize";
	public static final int DEFAULT_MONGODB_MAX_SIZE = 100;

	private MongoDBGlobalResource() {
		PDKIntegration.registerMemoryFetcher(this.getClass().getSimpleName(), this);
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
		return RESOURCE_MAP.computeIfAbsent(mongoClientKey, key -> new MongoClientPartition(mongoClientKey)).getMongoClientWithPartition(persistenceMongoDBConfig);
	}

	public void close(PersistenceMongoDBConfig persistenceMongoDBConfig) {
		if (null == persistenceMongoDBConfig) {
			return;
		}
		String mongoClientKey = getMongoClientKey(persistenceMongoDBConfig.getUri());
		RESOURCE_MAP.computeIfPresent(mongoClientKey, (key, value) -> {
			if (value.close(persistenceMongoDBConfig)) {
				return null;
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

	@Override
	public DataMap memory(String keyRegex, String memoryLevel) {
		DataMap dataMap = DataMap.create();
		DataMap resourceMap = DataMap.create();
		dataMap.kv("resources", resourceMap);
		RESOURCE_MAP.forEach((key, value) -> {
			DataMap partitionMap = DataMap.create();
			resourceMap.kv(key, partitionMap);
			Map<String, MongoClientHolder> mongoClientHolderMap = value.mongoClientHolderMap;
			DataMap holderMaps = DataMap.create();
			mongoClientHolderMap.forEach((code, holder) -> {
				DataMap holderMap = DataMap.create();
				DataMap configMaps = DataMap.create();
				holder.configs.forEach((name, config) -> {
					DataMap configMap = DataMap.create();
					configMap.kv("name", config.getName());
					configMap.kv("uri", config.getUri());
					configMap.kv("database", config.getDatabase());
					configMap.kv("collection", config.getCollection());
					configMaps.kv(config.getName(), configMap);
				});
				holderMap.kv("configs", configMaps);
				holderMap.kv("usage", holder.usage.get());
				holderMap.kv("code", holder.partitionCode);
				holderMap.kv("create client count", holder.createClientCounter.get());
				holderMap.kv("max size", holder.maxSize);
				holderMaps.kv(holder.partitionCode + "", holderMap);
			});
			partitionMap.kv("partitionSize", value.getPartitionSize());
			partitionMap.kv("holders", holderMaps);
			partitionMap.kv("create holder count", value.createHolderCounter.get());
		});
		return dataMap;
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
		private final Map<String, MongoClientHolder> mongoClientHolderMap = new ConcurrentHashMap<>();
		private int partitionSize = DEFAULT_PARTITION_SIZE;
		private String mongoClientKey;
		private AtomicInteger createHolderCounter = new AtomicInteger();

		public MongoClientPartition(String mongoClientKey) {
			this.mongoClientKey = mongoClientKey;
		}

		public MongoClientPartition partitionSize(int partitionSize) {
			this.partitionSize = partitionSize;
			return this;
		}

		public int getPartitionSize() {
			return partitionSize;
		}

		public MongoClient getMongoClientWithPartition(PersistenceMongoDBConfig persistenceMongoDBConfig) {
			int partitionCode = getPartitionCode(persistenceMongoDBConfig);
			return mongoClientHolderMap.computeIfAbsent(String.valueOf(partitionCode), key -> {
						MongoClientHolder mongoClientHolder = new MongoClientHolder(persistenceMongoDBConfig, this, partitionCode);
						createHolderCounter.incrementAndGet();
						return mongoClientHolder;
					})
					.addConfig(persistenceMongoDBConfig)
					.getMongoClient();
		}
		private int getPartitionCode(PersistenceMongoDBConfig persistenceMongoDBConfig) {
			String name = persistenceMongoDBConfig.getName();
			int hash = Math.abs(Objects.hash(name));
			return hash % partitionSize;
		}

		public boolean close(PersistenceMongoDBConfig persistenceMongoDBConfig) {
			int partitionCode = getPartitionCode(persistenceMongoDBConfig);
			mongoClientHolderMap.computeIfPresent(String.valueOf(partitionCode), (k, v) -> {
				if (v.close()) {
					return null;
				}
				return v;
			});
			return MapUtils.isEmpty(mongoClientHolderMap);
		}
	}

	private static class MongoClientHolder {
		private final AtomicInteger usage = new AtomicInteger(0);
		private final PersistenceMongoDBConfig persistenceMongoDBConfig;
		private final Map<String, PersistenceMongoDBConfig> configs;
		private final int maxSize;
		private MongoClient mongoClient;
		private MongoClientPartition mongoClientPartition;
		private final Lock lock = new ReentrantLock();
		private int partitionCode;
		private AtomicInteger createClientCounter = new AtomicInteger();

		public MongoClientHolder(PersistenceMongoDBConfig persistenceMongoDBConfig, MongoClientPartition mongoClientPartition, int partitionCode) {
			this.persistenceMongoDBConfig = persistenceMongoDBConfig;
			this.mongoClientPartition = mongoClientPartition;
			this.partitionCode = partitionCode;
			this.configs = new HashMap<>();
			this.maxSize = CommonUtils.getPropertyInt(MONGODB_MAX_SIZE, DEFAULT_MONGODB_MAX_SIZE);
			addConfig(persistenceMongoDBConfig);
		}

		public MongoClient getMongoClient() {
			try {
				lock.lock();
				if (null == mongoClient) {
					String uri = persistenceMongoDBConfig.getUri();
					MongoClientSettings.Builder mongoClientSettingBuilder = MongoClientSettings.builder();
					setSSLSettingIfNeed(mongoClientSettingBuilder);
					mongoClientSettingBuilder.applyToConnectionPoolSettings(connectionPoolSettings -> connectionPoolSettings.minSize(1).maxSize(maxSize));
					mongoClient = MongodbUtil.createClient(uri, mongoClientSettingBuilder.build());
					createClientCounter.incrementAndGet();
				}
				usage.incrementAndGet();
				return mongoClient;
			} finally {
				lock.unlock();
			}
		}

		private void setSSLSettingIfNeed(MongoClientSettings.Builder mongoClientSettingBuilder) {
			try {
				String uri = persistenceMongoDBConfig.getUri();
				boolean isSSL = persistenceMongoDBConfig.isSsl();
				if (!isSSL) {
					Pattern pattern = Pattern.compile("(ssl|tls)=true", Pattern.CASE_INSENSITIVE);
					Matcher matcher = pattern.matcher(uri);
					if (matcher.find()) {
						isSSL = true;
					}
				}
				if (isSSL) {
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

		MongoClientHolder addConfig(PersistenceMongoDBConfig config) {
			if (null == config) {
				return this;
			}
			this.configs.put(config.getName(), config);
			return this;
		}

		public boolean close() {
			try {
				lock.lock();
				if (usage.decrementAndGet() <= 0 && null != mongoClient) {
					mongoClient.close();
					mongoClient = null;
					return true;
				}
				return false;
			} finally {
				lock.unlock();
			}
		}
	}
}
