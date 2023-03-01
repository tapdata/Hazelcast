package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataPersistenceConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.config.HazelcastStoreConfig;
import com.hazelcast.persistence.config.PersistenceMongoDBConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.resource.ExternalResourceFactory;
import com.hazelcast.persistence.store.PersistenceMapStore;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import com.hazelcast.persistence.store.PersistenceStorageStore;
import com.hazelcast.persistence.store.PersistenceStoreFactory;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class PersistenceStorage {
	private final ConcurrentHashMap<String, Thread> ttlThreadMap = new ConcurrentHashMap<>();
	private Logger logger;
	private final ConcurrentHashMap<String, PersistenceStorageAbstractConfig> persistenceConfigMap = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>> storeImplementationMap = new ConcurrentHashMap<>();

	private PersistenceStorage() {
	}

	public static PersistenceStorage getInstance() {
		return PersistenceStorageSingleton.INSTANCE.getInstance();
	}

	private enum PersistenceStorageSingleton {
		INSTANCE;

		private final PersistenceStorage persistenceStorage;

		public PersistenceStorage getInstance() {
			return persistenceStorage;
		}

		PersistenceStorageSingleton() {
			this.persistenceStorage = new PersistenceStorage();
		}
	}

	public static String getConfigKey(ConstructType constructType, String name) {
		if (null == constructType) {
			throw new IllegalArgumentException("Construct type cannot be null");
		}
		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Name cannot be blank");
		}
		return String.join("-", constructType.name(), name);
	}

	public PersistenceStorageAbstractConfig getPersistenceStorageConfig(ConstructType constructType, String name) {
		String configKey = getConfigKey(constructType, name);
		return persistenceConfigMap.get(configKey);
	}

	public synchronized void addConfig(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
		String configKey;
		try {
			configKey = getConfigKey(persistenceStorageAbstractConfig.getConstructType(), persistenceStorageAbstractConfig.getName());
		} catch (Exception e) {
			throw new RuntimeException("Get config key failed", e);
		}
		PersistenceStorageAbstractConfig existingConfig = persistenceConfigMap.get(configKey);
		if (null != existingConfig) {
			if (!existingConfig.getStorageMode().equals(persistenceStorageAbstractConfig.getStorageMode())) {
				// Nonsupport change storage mode
				throw new RuntimeException("Change persistence storage mode is not allowed until restart instance\n old: " + existingConfig + "\n new: " + persistenceStorageAbstractConfig);
			}
		}
		persistenceConfigMap.put(configKey, persistenceStorageAbstractConfig);
	}

	public PersistenceStorage logger(Logger logger) {
		this.logger = logger;
		return this;
	}

	public PersistenceStorage initMapStoreConfig(Config c) {
		return initMapStoreConfig(c, "default");
	}

	public PersistenceStorage initMapStoreConfig(Config c, String mapName) {
		checkInitConfig(mapName, ConstructType.IMAP);
		PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(ConstructType.IMAP, mapName);
		if (null == persistenceStorageAbstractConfig) {
			throw new IllegalArgumentException(String.format("IMap name %s's persistence storage config is not exists, please add config", mapName));
		}
		StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
		MapConfig mapCfg = c.getMapConfig(mapName);
		MapStoreConfig mapStoreCfg = mapCfg.getMapStoreConfig();

		ExternalResource<PersistenceStorageAbstractConfig> externalResource = getExternalResource(storageMode);
		if (null == externalResource) {
			return this;
		}
		boolean initResult;
		try {
			HazelcastStoreConfig<MapStoreConfig> hazelcastStoreConfig = new HazelcastStoreConfig<>(mapStoreCfg);
			initResult = initStore(
					ConstructType.IMAP,
					mapName,
					persistenceStorageAbstractConfig,
					externalResource,
					hazelcastStoreConfig
			);
		} catch (Exception e) {
			CommonUtils.ignoreAnyError(externalResource::close);
			throw new RuntimeException(e);
		}
		EvictionConfig evictionConfig = new EvictionConfig()
				.setEvictionPolicy(EvictionPolicy.LRU)
				.setMaxSizePolicy(MaxSizePolicy.PER_NODE)
				.setSize(persistenceStorageAbstractConfig.getInMemSize());
		mapCfg.setEvictionConfig(evictionConfig);
		if (initResult) {
			mapStoreCfg.setEnabled(true);
			mapCfg.setMapStoreConfig(mapStoreCfg);
			mapCfg.setDataPersistenceConfig(new DataPersistenceConfig().setEnabled(true));
		}
		c.addMapConfig(mapCfg);
		return this;
	}

	public void destroy(String name) {
		CommonUtils.ignoreAnyError(() -> Optional.ofNullable(storeImplementationMap.get(name)).ifPresent(PersistenceStorageStore::doDestroy));
	}

	public PersistenceStorage initRingBufferConfig(Config c) {
		return initRingBufferConfig(c, "default");
	}

	public PersistenceStorage initRingBufferConfig(Config c, String ringBufferName) {
		checkInitConfig(ringBufferName, ConstructType.RINGBUFFER);
		PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(ConstructType.RINGBUFFER, ringBufferName);
		if (null == persistenceStorageAbstractConfig) {
			throw new IllegalArgumentException(String.format("Ring buffer name %s's persistence storage config is not exists, please add config", ringBufferName));
		}
		StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
		RingbufferConfig ringbufferConfig = c.getRingbufferConfig(ringBufferName);
		RingbufferStoreConfig ringbufferStoreConfig = ringbufferConfig.getRingbufferStoreConfig();

		ExternalResource<PersistenceStorageAbstractConfig> externalResource = getExternalResource(storageMode);
		if (null == externalResource) {
			return this;
		}
		boolean initResult;
		try {
			HazelcastStoreConfig<RingbufferStoreConfig> hazelcastStoreConfig = new HazelcastStoreConfig<>(ringbufferStoreConfig);
			initResult = initStore(
					ConstructType.RINGBUFFER,
					ringBufferName,
					persistenceStorageAbstractConfig,
					externalResource,
					hazelcastStoreConfig
			);
		} catch (Exception e) {
			CommonUtils.ignoreAnyError(externalResource::close);
			throw new RuntimeException(e);
		}
		ringbufferConfig.setCapacity(persistenceStorageAbstractConfig.getInMemSize())
				.setInMemoryFormat(InMemoryFormat.OBJECT);
		if (initResult) {
			ringbufferStoreConfig.setEnabled(true);
			ringbufferConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
		}
		c.addRingBufferConfig(ringbufferConfig);
		return this;
	}

	private synchronized boolean initStore(ConstructType constructType,
										   String name,
										   PersistenceStorageAbstractConfig persistenceStorageAbstractConfig,
										   ExternalResource<PersistenceStorageAbstractConfig> externalResource,
										   HazelcastStoreConfig<?> hazelcastStoreConfig) {
		String configKey = getConfigKey(constructType, name);
		StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
		if (storageMode == StorageMode.Mem && storeImplementationMap.containsKey(configKey)) {
			storeImplementationMap.get(configKey).disable();
		}
		if (storeImplementationMap.containsKey(configKey)) {
			PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = storeImplementationMap.get(configKey);
			if (!store.configEquals(persistenceStorageAbstractConfig)) {
				store.doDestroy();
				externalResource.doInit(persistenceStorageAbstractConfig);
				store.doInit(persistenceStorageAbstractConfig, externalResource);
			}
			store.enable();
		} else {
			PersistenceStoreFactory persistenceStoreFactory = new PersistenceStoreFactory();
			externalResource.doInit(persistenceStorageAbstractConfig);
			PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = persistenceStoreFactory.createStore(
					constructType,
					persistenceStorageAbstractConfig.getStorageMode()
			);
			if (null == store) {
				return false;
			}
			store.doInit(persistenceStorageAbstractConfig, externalResource);
			hazelcastStoreConfig.implementation(store);
			storeImplementationMap.put(configKey, store);
		}
		return true;
	}

	private static void checkInitConfig(String name, ConstructType constructType) {
		if (StringUtils.isBlank(name) || "default".equals(name)) {
			throw new RuntimeException(String.format("Default %s config is not allowed", constructType.name()));
		}
	}

	private static ExternalResource<PersistenceStorageAbstractConfig> getExternalResource(StorageMode storageMode) {
		ExternalResourceFactory externalResourceFactory = new ExternalResourceFactory();
		return externalResourceFactory.createExternalResource(storageMode);
	}

	private static PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> getStore(StorageMode storageMode) {
		PersistenceStoreFactory persistenceStoreFactory = new PersistenceStoreFactory();
		return persistenceStoreFactory.createStore(ConstructType.RINGBUFFER, storageMode);
	}


	public PersistenceStorage initHZConfig(Config c) {
		this.initMapStoreConfig(c);
		this.initRingBufferConfig(c);
		return this;
	}

	public PersistenceStorage initHZConfig(Config c, String configName) {
		this.initMapStoreConfig(c, configName);
		this.initRingBufferConfig(c, configName);
		return this;
	}

	public PersistenceStorage setImapTTL(IMap<String, Object> imap, long ttlSeconds) {
		ConstructType constructType = ConstructType.IMAP;
		PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(constructType, imap.getName());
		StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
		if (storageMode == StorageMode.Mem || storageMode == StorageMode.HTTP_TM) {
			return this;
		}
		String ttlThreadKey = getTtlThreadKey(constructType, imap.getName());
		if (ttlThreadMap.containsKey(ttlThreadKey)) {
			// use thread's interrupt method to stop pre ttl thread
			ttlThreadMap.get(ttlThreadKey).interrupt();
		}
		Thread ttlThread = new Thread(() -> {
			Thread.currentThread().setName(String.format("Clear-IMap-TTL-%s", ttlThreadKey));
			long sleepSeconds = 60;
			if (ttlSeconds < 60) {
				sleepSeconds = ttlSeconds;
			}
			if (sleepSeconds < 10) {
				sleepSeconds = 10;
			}
			PersistenceMapStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> persistenceMapStore = null;
			try {
				PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = createStore(persistenceStorageAbstractConfig);
				if (store instanceof PersistenceMapStore) {
					persistenceMapStore = (PersistenceMapStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>) store;
				} else {
					return;
				}
				while (ttlIsRunning()) {
					try {
						TimeUnit.SECONDS.sleep(sleepSeconds);
					} catch (InterruptedException e) {
						break;
					}
					try {
						Iterator<Map.Entry<String, Object>> iterator = imap.iterator();
						while (ttlIsRunning() && iterator.hasNext()) {
							Map.Entry<String, Object> entry = iterator.next();
							Object value = entry.getValue();
							if (!(value instanceof Document)) {
								continue;
							}
							Document document = (Document) entry.getValue();
							Long _ts = getTs(document);
							if (System.currentTimeMillis() - _ts * 1000 < ttlSeconds * 1000) {
								break;
							}
							persistenceMapStore.delete(entry.getKey());
						}
					} catch (Exception e) {
						if (null != logger) {
							logger.warn("IMap [{}] clear ttl data failed, ttl seconds: {}", imap.getName(), ttlSeconds, e);
						}
					}
				}
			} finally {
				Optional.ofNullable(persistenceMapStore).ifPresent(PersistenceStorageStore::doDestroy);
			}
		});
		ttlThread.start();
		ttlThreadMap.put(ttlThreadKey, ttlThread);
		return this;
	}

	public PersistenceStorage setRingBufferTTL(Ringbuffer<Document> rb, long ttlSeconds) {
		ConstructType constructType = ConstructType.RINGBUFFER;
		PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(constructType, rb.getName());
		StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
		if (storageMode == StorageMode.Mem || storageMode == StorageMode.HTTP_TM) {
			return this;
		}
		String ttlThreadKey = getTtlThreadKey(constructType, rb.getName());
		if (ttlThreadMap.containsKey(ttlThreadKey)) {
			// use thread's interrupt method to stop pre ttl thread
			ttlThreadMap.get(ttlThreadKey).interrupt();
		}
		Thread ttlThread = new Thread(() -> {
			Thread.currentThread().setName(String.format("Clear-RingBuffer-TTL-%s-%s", storageMode.name(), rb.getName()));
			long sleepSeconds = 60;
			if (ttlSeconds < 60) {
				sleepSeconds = ttlSeconds;
			}
			if (sleepSeconds < 10) {
				sleepSeconds = 10;
			}
			PersistenceRingBufferStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> persistenceRingBufferStore = null;
			try {
				PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = createStore(persistenceStorageAbstractConfig);
				if (store instanceof PersistenceRingBufferStore) {
					persistenceRingBufferStore = (PersistenceRingBufferStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>) store;
				} else {
					return;
				}
				while (ttlIsRunning()) {
					try {
						TimeUnit.SECONDS.sleep(sleepSeconds);
					} catch (InterruptedException e) {
						break;
					}
					try {
						if (rb.tailSequence() < 0) {
							continue;
						}
						long s = rb.headSequence() - 1;
						while (ttlIsRunning()) {
							s++;
							if (s >= rb.tailSequence()) {
								break;
							}
							Document document = null;
							try {
								Object obj = persistenceRingBufferStore.load(s);
								if (obj instanceof Document) {
									document = (Document) obj;
								}
							} catch (Exception e) {
								throw new RuntimeException("Read one from ringBuffer failed, sequence: " + s, e);
							}
							if (null == document) {
								continue;
							}
							Long _ts = getTs(document);
							if (_ts == null) continue;
							if (System.currentTimeMillis() - _ts * 1000 < ttlSeconds * 1000) {
								break;
							}
							persistenceRingBufferStore.delete(s);
						}
					} catch (Exception e) {
						if (null != logger) {
							logger.warn("Ringbuffer [{}] clear ttl data failed, ttl seconds: {}", rb.getName(), ttlSeconds, e);
						}
					}
				}
			} finally {
				Optional.ofNullable(persistenceRingBufferStore).ifPresent(PersistenceStorageStore::doDestroy);
			}
		});
		ttlThread.start();
		ttlThreadMap.put(ttlThreadKey, ttlThread);
		return this;
	}

	private static Long getTs(Document document) {
		if (null == document) {
			return null;
		}
		Document value = null;
		if (document.get("value") instanceof Document) {
			value = (Document) document.get("value");
		}
		if (null == value) {
			return null;
		}
		long _ts;
		if (!value.containsKey("_ts")) {
			return null;
		}
		_ts = value.getLong("_ts");
		return _ts;
	}

	private static PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> createStore(
			PersistenceStorageAbstractConfig persistenceStorageAbstractConfig
	) {
		if (null == persistenceStorageAbstractConfig) {
			return null;
		}
		ExternalResource<PersistenceStorageAbstractConfig> externalResource = new ExternalResourceFactory().createExternalResource(persistenceStorageAbstractConfig.getStorageMode());
		if (null == externalResource) return null;
		externalResource.doInit(persistenceStorageAbstractConfig);
		PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = new PersistenceStoreFactory().createStore(
				persistenceStorageAbstractConfig.getConstructType(),
				persistenceStorageAbstractConfig.getStorageMode()
		);
		if (null == store) return null;
		store.doInit(persistenceStorageAbstractConfig, externalResource);
		return store;
	}

	private static String getTtlThreadKey(ConstructType constructType, String constructName) {
		return getConfigKey(constructType, constructName);
	}

	private boolean ttlIsRunning() {
		return !Thread.currentThread().isInterrupted();
	}

	public long findSequence(Ringbuffer<Document> rb, long timestamp) {
		PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(ConstructType.RINGBUFFER, rb.getName());
		StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
		if (storageMode == StorageMode.MongoDB) {
			PersistenceMongoDBConfig persistenceMongoDBConfig = (PersistenceMongoDBConfig) persistenceStorageAbstractConfig;
			try (MongoClient mongoClient = new MongoClient(new MongoClientURI(persistenceMongoDBConfig.getUri()))) {
				MongoCollection<Document> cacheCollection = mongoClient.getDatabase(persistenceMongoDBConfig.getDatabase()).getCollection(persistenceMongoDBConfig.getCollection());
				Document query = new Document("ringBuffer", rb.getName()).append("value.timestamp", new Document("$gte", timestamp));
				Document document = cacheCollection.find(query).sort(ascending("_id")).first();
				if (document == null) {
					query = new Document("ringBuffer", rb.getName());
					document = cacheCollection.find(query).sort(descending("_id")).first();
					if (document == null) {
						return 0;
					}
					return document.getLong("key") + 1L;
				}
				return document.getLong("key");
			}
		}

		if (storageMode == StorageMode.RocksDB) {
			if (rb.tailSequence() == -1) {
				return 0;
			}
			for (long i = rb.headSequence(); i <= rb.tailSequence(); i++) {
				try {
					Document document = rb.readOne(i);
					if (document == null) {
						continue;
					}
					if (document.getLong("timestamp") >= timestamp) {
						return i;
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
		return rb.tailSequence() + 1L;
	}
}
