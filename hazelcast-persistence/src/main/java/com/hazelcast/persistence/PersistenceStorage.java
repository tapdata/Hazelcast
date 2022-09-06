package com.hazelcast.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.persistence.ConfigConstant.MONGO_COLLECTION;
import static com.hazelcast.persistence.ConfigConstant.MONGO_DB;
import static com.hazelcast.persistence.ConfigConstant.MONGO_URI;
import static com.hazelcast.persistence.ConfigConstant.ROCKSDB_DBPATH;
import static com.hazelcast.persistence.ConfigConstant.STORAGE_MODE;
import static com.hazelcast.persistence.StorageMode.MongoDB;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class PersistenceStorage {
	private StorageMode imapStorageMode = StorageMode.RocksDB;
	private static final String DEFAULT_IMAP_ROCKSDB_PATH = "./imap-cache-data/";
	private static final String DEFAULT_IMAP_MONGO_URI = "mongodb://127.0.0.1";
	private static final String DEFAULT_IMAP_DB = "cache";
	private static final String DEFAULT_IMAP_COLLECTION = "imap";
	private static final Integer DEFAULT_IMAP_IN_MEM_SIZE = 1;

	private StorageMode ringBufferStorageMode = StorageMode.RocksDB;
	private static final String DEFAULT_RING_BUFFER_ROCKSDB_PATH = "./ringBuffer-cache-data/";
	private static final String DEFAULT_RING_BUFFER_MONGO_URI = "mongodb://127.0.0.1";
	private static final String DEFAULT_RING_BUFFER_DB = "cache";
	private static final String DEFAULT_RING_BUFFER_COLLECTION = "ringBuffer";
	private static final Integer DEFAULT_RING_BUFFER_IN_MEM_SIZE = 1;
	private static final String DEFAULT_CONFIG_NAME = "default";

	private Config config;

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

	public Config getConfig() {
		return this.config;
	}
//	public PersistenceStorage setStorageMode(StorageMode storageMode) {
//		this.setImapStorageMode(storageMode);
//		this.setRingBufferStorageMode(storageMode);
//		return this;
//	}

//	public PersistenceStorage setImapStorageMode(StorageMode storageMode) {
//		this.imapStorageMode = storageMode;
//		return this;
//	}
//
//	public PersistenceStorage setRingBufferStorageMode(StorageMode storageMode) {
//		this.ringBufferStorageMode = storageMode;
//		return this;
//	}

//	public PersistenceStorage setRocksDBPath(String rocksDBPath) {
//		this.setImapRocksDBPath(rocksDBPath);
//		this.setRingBufferRocksDBPath(rocksDBPath);
//		return this;
//	}

//	public PersistenceStorage setImapRocksDBPath(String rocksDBPath) {
//		this.imapRocksDBPath = rocksDBPath;
//		return this;
//	}
//
//	public PersistenceStorage setRingBufferRocksDBPath(String rocksDBPath) {
//		this.ringBufferRocksDBPath = rocksDBPath;
//		return this;
//	}
//
//	public PersistenceStorage setMongoUri(String mongoUri) {
//		this.setImapMongoUri(mongoUri);
//		this.setRingBufferMongoUri(mongoUri);
//		return this;
//	}

//	public PersistenceStorage setImapMongoUri(String mongoUri) {
//		this.imapMongoUri = mongoUri;
//		return this;
//	}

//	public String getImapMongoUri() {
//		return this.imapMongoUri;
//	}

//	public PersistenceStorage setRingBufferMongoUri(String mongoUri) {
//		this.ringBufferMongoUri = mongoUri;
//		return this;
//	}

//	public PersistenceStorage setDB(String db) {
//		this.setImapDB(db);
//		this.setRingBufferDB(db);
//		return this;
//	}

//	public PersistenceStorage setImapDB(String db) {
//		this.imapDB = db;
//		return this;
//	}
//
//	public PersistenceStorage setRingBufferDB(String db) {
//		this.ringBufferDB = db;
//		return this;
//	}

//	public PersistenceStorage setCollection(String collection) {
//		this.setImapCollection(collection);
//		this.setRingBufferCollection((collection));
//		return this;
//	}

//	public PersistenceStorage setImapCollection(String imapCollection) {
//		this.imapCollection = imapCollection;
//		return this;
//	}
//
//	public PersistenceStorage setRingBufferCollection(String ringBufferCollection) {
//		this.ringBufferCollection = ringBufferCollection;
//		return this;
//	}
//
//	public PersistenceStorage setInMemSize(Integer inMemSize) {
//		this.setImapInMemSize(inMemSize);
//		this.setRingBufferInMemSize(inMemSize);
//		return this;
//	}

//	public PersistenceStorage setImapInMemSize(Integer imapInMemSize) {
//		this.imapInMemSize = imapInMemSize;
//		return this;
//	}
//
//	public PersistenceStorage setRingBufferInMemSize(Integer ringBufferInMemSize) {
//		this.ringBufferInMemSize = ringBufferInMemSize;
//		return this;
//	}

	private PersistenceStorage initMapStoreConfig(Config c) {
		return initMapStoreConfig(c, DEFAULT_CONFIG_NAME);
	}

	public PersistenceStorage initMapStoreConfig(Config c, String mapName) {
		if (this.imapStorageMode == StorageMode.Mem) {
			return this;
		}
		MapConfig mapCfg = c.getMapConfig(mapName);
		MapStoreConfig mapStoreCfg = mapCfg.getMapStoreConfig();
		switch (this.imapStorageMode) {
			case MongoDB:
				mapStoreCfg.setClassName(MongoDBIMap.class.getName())
						.setProperty(MONGO_URI, DEFAULT_IMAP_MONGO_URI)
						.setProperty(MONGO_DB, DEFAULT_IMAP_DB)
						.setProperty(MONGO_COLLECTION, DEFAULT_IMAP_COLLECTION);
				break;
			case RocksDB:
				mapStoreCfg.setClassName(RocksDBIMap.class.getName())
						.setProperty(ROCKSDB_DBPATH, DEFAULT_IMAP_ROCKSDB_PATH);
		}
		EvictionConfig evictionConfig = new EvictionConfig()
				.setEvictionPolicy(EvictionPolicy.LRU)
				.setMaxSizePolicy(MaxSizePolicy.PER_NODE)
				.setSize(DEFAULT_IMAP_IN_MEM_SIZE);
		mapCfg.setEvictionConfig(evictionConfig);
		mapStoreCfg.setEnabled(true);
		mapCfg.setMapStoreConfig(mapStoreCfg);
		c.addMapConfig(mapCfg);
		return this;
	}

	private PersistenceStorage initRingBufferConfig(Config c) {
		return initRingBufferConfig(c, DEFAULT_CONFIG_NAME);
	}

	public PersistenceStorage initRingBufferConfig(Config c, String ringBufferName) {
		if (this.ringBufferStorageMode == StorageMode.Mem) {
			return this;
		}

		RingbufferConfig ringbufferConfig = c.getRingbufferConfig(ringBufferName);
		ringbufferConfig.setCapacity(DEFAULT_RING_BUFFER_IN_MEM_SIZE);
		RingbufferStoreConfig ringbufferStoreConfig = ringbufferConfig.getRingbufferStoreConfig();
		switch (this.ringBufferStorageMode) {
			case MongoDB:
				ringbufferStoreConfig.setClassName(MongoDBRingBuffer.class.getName())
						.setProperty(MONGO_URI, DEFAULT_RING_BUFFER_MONGO_URI)
						.setProperty(MONGO_DB, DEFAULT_RING_BUFFER_DB)
						.setProperty(MONGO_COLLECTION, DEFAULT_RING_BUFFER_COLLECTION);
				break;
			case RocksDB:
				ringbufferStoreConfig.setClassName(RocksDBRingBuffer.class.getName())
						.setProperty(ROCKSDB_DBPATH, DEFAULT_RING_BUFFER_ROCKSDB_PATH);
		}
		ringbufferConfig.setCapacity(DEFAULT_RING_BUFFER_IN_MEM_SIZE).setInMemoryFormat(InMemoryFormat.OBJECT);
		ringbufferStoreConfig.setEnabled(true);
		ringbufferConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
		c.addRingBufferConfig(ringbufferConfig);
		return this;
	}


	public PersistenceStorage initHZConfig(Config c) {
		this.initMapStoreConfig(c);
		this.initRingBufferConfig(c);
		this.config = c;
		return this;
	}

	public PersistenceStorage initHZConfig(Config c, String configName) {
		this.initMapStoreConfig(c, configName);
		this.initRingBufferConfig(c, configName);
		this.config = c;
		return this;
	}

	/**
	 *
	 * @param newConfig
	 */
	public void addConfig(final ExternalStorageConfig newConfig) {
		if (newConfig == null || !newConfig.checkIfValid()) {
			throw new RuntimeException("invalid config");
		}
		final Object config = newConfig.getConfig();
		final String configName = newConfig.getConfigName();
		final StorageMode storageMode = newConfig.getStorageMode();
		if (config instanceof MongoDBConfig) {
			final MongoDBConfig mongoDBConfig = (MongoDBConfig) config;

			final MapConfig mapConfig = new MapConfig();
			mapConfig.setName(configName);
			mapConfig.getMapStoreConfig()
					.setClassName(MongoDBIMap.class.getName())
					.setProperty(MONGO_URI, mongoDBConfig.getMongoUrl())
					.setProperty(MONGO_COLLECTION, mongoDBConfig.getCollection())
					.setProperty(MONGO_DB, mongoDBConfig.getDb())
					.setProperty(STORAGE_MODE, storageMode.name())
					.setEnabled(true);
			this.config.addMapConfig(mapConfig);

			final RingbufferConfig ringbufferConfig = new RingbufferConfig();
			ringbufferConfig.setName(configName);
			ringbufferConfig.getRingbufferStoreConfig()
					.setClassName(MongoDBRingBuffer.class.getName())
					.setProperty(MONGO_URI, mongoDBConfig.getMongoUrl())
					.setProperty(MONGO_COLLECTION, mongoDBConfig.getCollection())
					.setProperty(MONGO_DB, mongoDBConfig.getDb())
					.setProperty(STORAGE_MODE, storageMode.name())
					.setEnabled(true);
			this.config.addRingBufferConfig(ringbufferConfig);
		} else if (config instanceof RocksDBConfig) {
			final RocksDBConfig rocksDBConfig = (RocksDBConfig) config;

			final MapConfig mapConfig = new MapConfig();
			mapConfig.setName(configName);
			mapConfig.getMapStoreConfig()
					.setClassName(RocksDBIMap.class.getName())
					.setProperty(ROCKSDB_DBPATH, rocksDBConfig.getDbPath())
					.setProperty(STORAGE_MODE, storageMode.name())
					.setEnabled(true);
			this.config.addMapConfig(mapConfig);

			final RingbufferConfig ringbufferConfig = new RingbufferConfig();
			ringbufferConfig.setName(configName);
			ringbufferConfig.getRingbufferStoreConfig()
					.setClassName(RocksDBRingBuffer.class.getName())
					.setProperty(ROCKSDB_DBPATH, rocksDBConfig.getDbPath())
					.setProperty(STORAGE_MODE, storageMode.name())
					.setEnabled(true);
			this.config.addRingBufferConfig(ringbufferConfig);
		} else {
			throw new UnsupportedOperationException("unsupported config type");
		}
	}

	public void modifyConfig(final ExternalStorageConfig newConfig) {
		if (newConfig == null || !newConfig.checkIfValid()) {
			throw new RuntimeException("invalid config");
		}
		final String configName = newConfig.getConfigName();
		final MapConfig mapConfig = this.config.getMapConfigOrNull(configName);
		if (mapConfig == null) {
			throw new RuntimeException("specific config does not exist");
		}
		final MapConfig existedMapConfig = this.config.getMapConfig(configName);
		final MapStoreConfig mapStoreConfig = existedMapConfig.getMapStoreConfig();
		final RingbufferConfig existedRbConfig = this.config.getRingbufferConfig(configName);
		final RingbufferStoreConfig rbStoreConfig = existedRbConfig.getRingbufferStoreConfig();

		final Object config = newConfig.getConfig();
		if (config instanceof MongoDBConfig) {
			final MongoDBConfig mongoDBConfig = (MongoDBConfig) config;
			mapStoreConfig.setProperty(MONGO_URI, mongoDBConfig.getMongoUrl())
					.setProperty(MONGO_COLLECTION, mongoDBConfig.getCollection())
					.setProperty(MONGO_DB, mongoDBConfig.getDb());

			rbStoreConfig.setProperty(MONGO_URI, mongoDBConfig.getMongoUrl())
					.setProperty(MONGO_COLLECTION, mongoDBConfig.getCollection())
					.setProperty(MONGO_DB, mongoDBConfig.getDb());
		} else if (config instanceof RocksDBConfig) {
			final RocksDBConfig rocksDBConfig = (RocksDBConfig) config;
			mapStoreConfig.setProperty(ROCKSDB_DBPATH, rocksDBConfig.getDbPath());
			rbStoreConfig.setProperty(ROCKSDB_DBPATH, rocksDBConfig.getDbPath());
		} else {
			throw new UnsupportedOperationException("unsupported config type");
		}
	}

	public void deleteConfig(final String configName) {

	}

	public List<ExternalStorageConfig> queryConfigs() {
		final Map<String, MapConfig> mapConfigs = this.config.getMapConfigs();
		final List<ExternalStorageConfig> configs = new ArrayList<>();
		for (final Map.Entry<String, MapConfig> entry : mapConfigs.entrySet()) {
			final String configName = entry.getKey();
			final MapConfig mapConfig = entry.getValue();
			final MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
			final String storageMode = mapStoreConfig.getProperty(STORAGE_MODE);
			final ExternalStorageConfig config = new ExternalStorageConfig();

			config.setConfigName(configName);
			if (StorageMode.MongoDB.name().equals(storageMode)) {
				config.setStorageMode(StorageMode.MongoDB);

				MongoDBConfig mongoDBConfig = new MongoDBConfig();
				mongoDBConfig.setMongoUrl(mapStoreConfig.getProperty(MONGO_URI));
				mongoDBConfig.setDb(mapStoreConfig.getProperty(MONGO_DB));
				mongoDBConfig.setCollection(mapStoreConfig.getProperty(MONGO_COLLECTION));
				config.setConfig(mongoDBConfig);
			} else if (StorageMode.RocksDB.name().equals(storageMode)) {
				config.setStorageMode(StorageMode.RocksDB);

				RocksDBConfig rocksDBConfig = new RocksDBConfig();
				rocksDBConfig.setDbPath(mapStoreConfig.getProperty(ROCKSDB_DBPATH));
				config.setConfig(rocksDBConfig);
			} else {
				config.setStorageMode(StorageMode.Mem);
			}
			configs.add(config);
		}
		return configs;
	}

//	public PersistenceStorage addHZConfig(Config c, String configName, StorageMode storageMode) {
//		if (configName == null || configName.isEmpty()) {
//			throw new UnsupportedOperationException("invalid config name");
//		}
//		if (DEFAULT_CONFIG_NAME.equals(configName)) {
//			this.initHZConfig(c, DEFAULT_CONFIG_NAME);
//		}
//		this.configMap.put(configName, c);
//		Map<String, MapConfig> map = c.getMapConfigs();
//
//		return this;
//	}

	public PersistenceStorage setImapTTL(String imapName, long ttlSeconds) {
		new Thread(() -> {
			long sleepSeconds = 60;
			if (ttlSeconds < 60) {
				sleepSeconds = ttlSeconds;
			}
			if (sleepSeconds < 10) {
				sleepSeconds = 10;
			}
			while (true) {
				try {
					Thread.sleep(sleepSeconds * 1000);
				} catch (Exception e) {
				}
			}
		}).start();
		return this;
	}

//	public PersistenceStorage setRingBufferTTL(Ringbuffer<Document> rb, long ttlSeconds) {
//		if (this.ringBufferStorageMode == StorageMode.Mem) {
//			return this;
//		}
//		new Thread(() -> {
//			long sleepSeconds = 60;
//			if (ttlSeconds < 60) {
//				sleepSeconds = ttlSeconds;
//			}
//			if (sleepSeconds < 10) {
//				sleepSeconds = 10;
//			}
//			RocksDB rocksDB = null;
//			MongoCollection<Document> cacheCollection = null;
//			String keySplit = "__0x1__";
//			String sign = rb.getName() + keySplit;
//			if (this.ringBufferStorageMode == StorageMode.RocksDB) {
//				rocksDB = RocksDBInstance.getInstance(this.ringBufferRocksDBPath);
//			}
//			if (this.ringBufferStorageMode == StorageMode.MongoDB) {
//				MongoClient mongoClient = new MongoClient(new MongoClientURI(this.ringBufferMongoUri));
//				cacheCollection = mongoClient.getDatabase(this.ringBufferDB).getCollection(this.ringBufferCollection);
//			}
//			while (true) {
//				try {
//					Thread.sleep(sleepSeconds * 1000);
//					if (rb.tailSequence() == -1) {
//						continue;
//					}
//					long s = rb.headSequence() - 1;
//					while (true) {
//						s++;
//						if (s >= rb.tailSequence()) {
//							break;
//						}
//						long _ts = 0;
//						try {
//							_ts = rb.readOne(s).getLong("_ts");
//						} catch (Exception e) {
//							break;
//						}
//						if (System.currentTimeMillis() - _ts * 1000 < ttlSeconds * 1000) {
//							break;
//						}
//						if (this.ringBufferStorageMode == StorageMode.RocksDB) {
//							try {
//								rocksDB.delete((sign + s).getBytes(StandardCharsets.UTF_8));
//								rocksDB.put((sign + "smallestSequence").getBytes(StandardCharsets.UTF_8), ((Long) (s + 1)).toString().getBytes());
//							} catch (RocksDBException e) {
//							}
//						}
//						if (this.ringBufferStorageMode == StorageMode.MongoDB) {
//							Document query = new Document("ringBuffer", rb.getName()).append("key", s);
//							cacheCollection.deleteOne(query);
//						}
//					}
//				} catch (Exception e) {
//				}
//			}
//		}).start();
//		return this;
//	}

	public PersistenceStorage setRingBufferTTL(Ringbuffer<Document> rb, long ttlSeconds, Config config, String configName) {
		final MapConfig mapConfig = config.getMapConfigOrNull(configName);
		if (mapConfig == null) {
			throw new RuntimeException("specific config does not exist");
		}
		final RingbufferConfig existedConfig = config.getRingbufferConfig(configName);
		final RingbufferStoreConfig storeConfig = existedConfig.getRingbufferStoreConfig();
		final String storageMode = storeConfig.getProperty(STORAGE_MODE);
		if (Objects.equals(storageMode, StorageMode.Mem.name())) {
			return this;
		}
		new Thread(() -> {
			long sleepSeconds = 60;
			if (ttlSeconds < 60) {
				sleepSeconds = ttlSeconds;
			}
			if (sleepSeconds < 10) {
				sleepSeconds = 10;
			}
			RocksDB rocksDB = null;
			MongoCollection<Document> cacheCollection = null;
			String keySplit = "__0x1__";
			String sign = rb.getName() + keySplit;
			if (Objects.equals(storageMode, StorageMode.RocksDB.name())) {
				final String rbRocksDBPath = storeConfig.getProperty(ROCKSDB_DBPATH);
				rocksDB = RocksDBInstance.getInstance(rbRocksDBPath);
			}
			if (Objects.equals(storageMode, MongoDB.name())) {
				final String rbMongoUri = storeConfig.getProperty(MONGO_URI);
				final String rbDB = storeConfig.getProperty(MONGO_DB);
				final String rbCollection = storeConfig.getProperty(MONGO_COLLECTION);
				MongoClient mongoClient = new MongoClient(new MongoClientURI(rbMongoUri));
				cacheCollection = mongoClient.getDatabase(rbDB).getCollection(rbCollection);
			}
			while (true) {
				try {
					Thread.sleep(sleepSeconds * 1000);
					if (rb.tailSequence() == -1) {
						continue;
					}
					long s = rb.headSequence() - 1;
					while (true) {
						s++;
						if (s >= rb.tailSequence()) {
							break;
						}
						long _ts = 0;
						try {
							_ts = rb.readOne(s).getLong("_ts");
						} catch (Exception e) {
							break;
						}
						if (System.currentTimeMillis() - _ts * 1000 < ttlSeconds * 1000) {
							break;
						}
						if (Objects.equals(storageMode, StorageMode.RocksDB.name())) {
							try {
								rocksDB.delete((sign + s).getBytes(StandardCharsets.UTF_8));
								rocksDB.put((sign + "smallestSequence").getBytes(StandardCharsets.UTF_8), ((Long) (s + 1)).toString().getBytes());
							} catch (RocksDBException e) {
							}
						}
						if (Objects.equals(storageMode, MongoDB.name())) {
							Document query = new Document("ringBuffer", rb.getName()).append("key", s);
							cacheCollection.deleteOne(query);
						}
					}
				} catch (Exception e) {
				}
			}
		}).start();
		return this;
	}

//	public long findSequence(Ringbuffer<Document> rb, long timestamp) {
//		if (this.ringBufferStorageMode == StorageMode.MongoDB) {
//			try (MongoClient mongoClient = new MongoClient(new MongoClientURI(this.ringBufferMongoUri))) {
//				MongoCollection<Document> cacheCollection = mongoClient.getDatabase(this.ringBufferDB).getCollection(this.ringBufferCollection);
//				Document query = new Document("ringBuffer", rb.getName()).append("value.timestamp", new Document("$gte", timestamp));
//				Document document = cacheCollection.find(query).sort(ascending("_id")).first();
//				if (document == null) {
//					query = new Document("ringBuffer", rb.getName());
//					document = cacheCollection.find(query).sort(descending("_id")).first();
//					if (document == null) {
//						return 0;
//					}
//					return document.getLong("key");
//				}
//				return document.getLong("key");
//			}
//		}
//
//		if (this.ringBufferStorageMode == StorageMode.RocksDB) {
//			if (rb.tailSequence() == -1) {
//				return 0;
//			}
//			for (long i = rb.headSequence(); i <= rb.tailSequence(); i++) {
//				try {
//					Document document = rb.readOne(i);
//					if (document == null) {
//						continue;
//					}
//					if (document.getLong("timestamp") >= timestamp) {
//						return i;
//					}
//				} catch (Exception e) {
//					continue;
//				}
//			}
//		}
//		return rb.tailSequence();
//	}

	public long findSequence(Ringbuffer<Document> rb, long timestamp, Config config, String configName) {
		final MapConfig mapConfig = config.getMapConfigOrNull(configName);
		if (mapConfig == null) {
			throw new RuntimeException("specific config does not exist");
		}
		final RingbufferConfig existedConfig = config.getRingbufferConfig(configName);
		final RingbufferStoreConfig storeConfig = existedConfig.getRingbufferStoreConfig();

		final String storageMode = storeConfig.getProperty(STORAGE_MODE);
		if (Objects.equals(storageMode, MongoDB.name())) {
			final String rbMongoUri = storeConfig.getProperty(MONGO_URI);
			final String rbMongoDB = storeConfig.getProperty(MONGO_DB);
			final String rbMongoCollection = storeConfig.getProperty(MONGO_COLLECTION);
			try (MongoClient mongoClient = new MongoClient(new MongoClientURI(rbMongoUri))) {
				MongoCollection<Document> cacheCollection = mongoClient.getDatabase(rbMongoDB).getCollection(rbMongoCollection);
				Document query = new Document("ringBuffer", rb.getName()).append("value.timestamp", new Document("$gte", timestamp));
				Document document = cacheCollection.find(query).sort(ascending("_id")).first();
				if (document == null) {
					query = new Document("ringBuffer", rb.getName());
					document = cacheCollection.find(query).sort(descending("_id")).first();
					if (document == null) {
						return 0;
					}
					return document.getLong("key");
				}
				return document.getLong("key");
			}
		}

		if (Objects.equals(storageMode, StorageMode.RocksDB.name())) {
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
				} catch (Exception ignored) {
				}
			}
		}
		return rb.tailSequence();
	}
}
