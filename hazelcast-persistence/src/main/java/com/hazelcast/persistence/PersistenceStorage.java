package com.hazelcast.persistence;

import com.hazelcast.config.*;
import com.hazelcast.persistence.http.HttpConstant;
import com.hazelcast.persistence.http.HttpTMIMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.logging.log4j.Logger;
import org.bson.BsonBinaryReader;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class PersistenceStorage {
	private StorageMode imapStorageMode = StorageMode.RocksDB;
	private String imapRocksDBPath = "./imap-cache-data/";
	private String imapMongoUri = "mongodb://127.0.0.1";
	private String imapDB = "cache";
	private String imapCollection = "imap";
	private Integer imapInMemSize = 1;

	private StorageMode ringBufferStorageMode = StorageMode.RocksDB;
	private String ringBufferRocksDBPath = "./ringBuffer-cache-data/";
	private String ringBufferMongoUri = "mongodb://127.0.0.1";
	private String ringBufferDB = "cache";
	private String ringBufferCollection = "ringBuffer";
	private Integer ringBufferInMemSize = 1;
	private String baseUrl;
	private String accessCode;
	private ConcurrentHashMap<String, Thread> ttlThreadMap = new ConcurrentHashMap<>();
	private Logger logger;

	public PersistenceStorage() {
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

	public PersistenceStorage setStorageMode(StorageMode storageMode) {
		this.setImapStorageMode(storageMode);
		this.setRingBufferStorageMode(storageMode);
		return this;
	}

	public PersistenceStorage setImapStorageMode(StorageMode storageMode) {
		this.imapStorageMode = storageMode;
		return this;
	}

	public PersistenceStorage setRingBufferStorageMode(StorageMode storageMode) {
		this.ringBufferStorageMode = storageMode;
		return this;
	}

	public PersistenceStorage setRocksDBPath(String rocksDBPath) {
		this.setImapRocksDBPath(rocksDBPath);
		this.setRingBufferRocksDBPath(rocksDBPath);
		return this;
	}

	public PersistenceStorage setImapRocksDBPath(String rocksDBPath) {
		this.imapRocksDBPath = rocksDBPath;
		return this;
	}

	public PersistenceStorage setRingBufferRocksDBPath(String rocksDBPath) {
		this.ringBufferRocksDBPath = rocksDBPath;
		return this;
	}

	public PersistenceStorage setMongoUri(String mongoUri) {
		this.setImapMongoUri(mongoUri);
		this.setRingBufferMongoUri(mongoUri);
		return this;
	}

	public PersistenceStorage setImapMongoUri(String mongoUri) {
		this.imapMongoUri = mongoUri;
		return this;
	}

	public PersistenceStorage setRingBufferMongoUri(String mongoUri) {
		this.ringBufferMongoUri = mongoUri;
		return this;
	}

	public PersistenceStorage setDB(String db) {
		this.setImapDB(db);
		this.setRingBufferDB(db);
		return this;
	}

	public PersistenceStorage setImapDB(String db) {
		this.imapDB = db;
		return this;
	}

	public PersistenceStorage setRingBufferDB(String db) {
		this.ringBufferDB = db;
		return this;
	}

	public PersistenceStorage setCollection(String collection) {
		this.setImapCollection(collection);
		this.setRingBufferCollection((collection));
		return this;
	}

	public PersistenceStorage setImapCollection(String imapCollection) {
		this.imapCollection = imapCollection;
		return this;
	}

	public PersistenceStorage setRingBufferCollection(String ringBufferCollection) {
		this.ringBufferCollection = ringBufferCollection;
		return this;
	}

	public PersistenceStorage setInMemSize(Integer inMemSize) {
		this.setImapInMemSize(inMemSize);
		this.setRingBufferInMemSize(inMemSize);
		return this;
	}

	public PersistenceStorage setImapInMemSize(Integer imapInMemSize) {
		this.imapInMemSize = imapInMemSize;
		return this;
	}

	public PersistenceStorage setRingBufferInMemSize(Integer ringBufferInMemSize) {
		this.ringBufferInMemSize = ringBufferInMemSize;
		return this;
	}

	public PersistenceStorage baseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
		return this;
	}

	public PersistenceStorage accessCode(String accessCode) {
		this.accessCode = accessCode;
		return this;
	}

	private Integer connectTimeoutMs;

	public PersistenceStorage connectTimeoutMs(Integer connectTimeoutMs) {
		this.connectTimeoutMs = connectTimeoutMs;
		return this;
	}

	private Integer readTimeoutMs;

	public PersistenceStorage readTimeoutMs(Integer readTimeoutMs) {
		this.readTimeoutMs = readTimeoutMs;
		return this;
	}

	public PersistenceStorage logger(Logger logger) {
		this.logger = logger;
		return this;
	}

	public PersistenceStorage initMapStoreConfig(Config c) {
		return initMapStoreConfig(c, "default");
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
						.setProperty("mongo.uri", this.imapMongoUri)
						.setProperty("mongo.db", this.imapDB)
						.setProperty("mongo.collection", this.imapCollection);
				break;
			case RocksDB:
				mapStoreCfg.setClassName(RocksDBIMap.class.getName())
						.setProperty("rocksdb.dbPath", this.imapRocksDBPath);
				break;
			case HTTP_TM:
				mapStoreCfg.setClassName(HttpTMIMap.class.getName())
						.setProperty(HttpConstant.BASE_URL_PROPERTY, baseUrl)
						.setProperty(HttpConstant.ACCESS_CODE_PROPERTY, accessCode)
						.setProperty(HttpConstant.CONNECT_TIMEOUT_PROPERTY, connectTimeoutMs.toString())
						.setProperty(HttpConstant.READ_TIMEOUT_PROPERTY, readTimeoutMs.toString());
				break;
		}
		EvictionConfig evictionConfig = new EvictionConfig()
				.setEvictionPolicy(EvictionPolicy.LRU)
				.setMaxSizePolicy(MaxSizePolicy.PER_NODE)
				.setSize(this.imapInMemSize);
		mapCfg.setEvictionConfig(evictionConfig);
		mapStoreCfg.setEnabled(true);
		mapCfg.setMapStoreConfig(mapStoreCfg);
		c.addMapConfig(mapCfg);
		return this;
	}

	public PersistenceStorage initRingBufferConfig(Config c) {
		return initRingBufferConfig(c, "default");
	}

	public PersistenceStorage initRingBufferConfig(Config c, String ringBufferName) {
		if (this.ringBufferStorageMode == StorageMode.Mem) {
			return this;
		}

		RingbufferConfig ringbufferConfig = c.getRingbufferConfig(ringBufferName);
		ringbufferConfig.setCapacity(this.ringBufferInMemSize);
		RingbufferStoreConfig ringbufferStoreConfig = ringbufferConfig.getRingbufferStoreConfig();
		switch (this.ringBufferStorageMode) {
			case MongoDB:
				ringbufferStoreConfig.setClassName(MongoDBRingBuffer.class.getName())
						.setProperty("mongo.uri", this.ringBufferMongoUri)
						.setProperty("mongo.db", this.ringBufferDB)
						.setProperty("mongo.collection", this.ringBufferCollection);
				break;
			case RocksDB:
				ringbufferStoreConfig.setClassName(RocksDBRingBuffer.class.getName())
						.setProperty("rocksdb.dbPath", this.ringBufferRocksDBPath);
		}
		ringbufferConfig.setCapacity(this.ringBufferInMemSize).setInMemoryFormat(InMemoryFormat.OBJECT);
		ringbufferStoreConfig.setEnabled(true);
		ringbufferConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
		c.addRingBufferConfig(ringbufferConfig);
		return this;
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

	public PersistenceStorage setRingBufferTTL(Ringbuffer<Document> rb, long ttlSeconds) {
		if (this.ringBufferStorageMode == StorageMode.Mem) {
			return this;
		}
		if (this.ringBufferStorageMode == StorageMode.HTTP_TM) {
			return this;
		}
		if (ttlThreadMap.containsKey(rb.getName())) {
			// use thread's interrupt method to stop pre ttl thread
			ttlThreadMap.get(rb.getName()).interrupt();
		}
		Thread ttlThread = new Thread(() -> {
			RocksDB rocksDB = null;
			MongoClient mongoClient = null;
			MongoCollection<Document> cacheCollection = null;
			Codec<Document> documentCodec = null;
				Thread.currentThread().setName(String.format("Clear-RingBuffer-TTL-%s-%s", ringBufferStorageMode.name(), rb.getName()));
				long sleepSeconds = 60;
				if (ttlSeconds < 60) {
					sleepSeconds = ttlSeconds;
				}
				if (sleepSeconds < 10) {
					sleepSeconds = 10;
				}
			try {
				String keySplit = "__0x1__";
				String sign = rb.getName() + keySplit;
				if (this.ringBufferStorageMode == StorageMode.MongoDB) {
					mongoClient = MongodbUtil.createClient(this.ringBufferMongoUri);
					cacheCollection = mongoClient.getDatabase(this.ringBufferDB).getCollection(this.ringBufferCollection);
				} else if (this.ringBufferStorageMode == StorageMode.RocksDB) {
					rocksDB = RocksDBInstance.getInstance(this.ringBufferRocksDBPath);
					documentCodec = new DocumentCodec();
				}
				while (ttlIsRunning()) {
					try {
						TimeUnit.SECONDS.sleep(sleepSeconds);
					} catch (InterruptedException e) {
						break;
					}
					try {
						if (rb.tailSequence() == -1) {
							continue;
						}
						long s = rb.headSequence() - 1;
						while (ttlIsRunning()) {
							s++;
							if (s >= rb.tailSequence()) {
								break;
							}
							Document document = null;
							Document value = null;
							try {
								if (this.ringBufferStorageMode == StorageMode.MongoDB) {
									document = cacheCollection.find(new Document("ringBuffer", rb.getName()).append("key", s)).first();
								} else if (this.ringBufferStorageMode == StorageMode.RocksDB) {
									byte[] bytes = rocksDB.get((sign + s).getBytes());
									if (null != bytes) {
										BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
										document = documentCodec.decode(bsonReader, DecoderContext.builder().build());
									}
								}
							} catch (Exception e) {
								throw new RuntimeException("Read one from ringBuffer failed, sequence: " + s, e);
							}
							if (null == document) {
								continue;
							}
							if (document.get("value") instanceof Document) {
								value = (Document) document.get("value");
							}
							if (null == value) {
								continue;
							}
							long _ts;
							if (!value.containsKey("_ts")) {
								continue;
							}
							_ts = value.getLong("_ts");
							if (System.currentTimeMillis() - _ts * 1000 < ttlSeconds * 1000) {
								break;
							}
							if (this.ringBufferStorageMode == StorageMode.MongoDB) {
								Document query = new Document("ringBuffer", rb.getName()).append("key", s);
								try {
									cacheCollection.deleteOne(query);
								} catch (Exception e) {
									throw new RuntimeException("Delete from mongodb failed, query: " + query.toJson(), e);
								}
							} else if (this.ringBufferStorageMode == StorageMode.RocksDB) {
								try {
									rocksDB.delete((sign + s).getBytes(StandardCharsets.UTF_8));
									rocksDB.put((sign + "smallestSequence").getBytes(StandardCharsets.UTF_8), ((Long) (s + 1)).toString().getBytes());
								} catch (RocksDBException e) {
									throw new RuntimeException("Delete from rocksdb failed, key: " + sign + s, e);
								}
							}
						}
					} catch (Exception e) {
						if (null != logger) {
							logger.warn("Ringbuffer [{}] clear ttl data failed, ttl seconds: {}", ttlSeconds, rb.getName(), e);
						}
					}
				}
			} finally {
				try {
					Optional.ofNullable(rocksDB).ifPresent(RocksDB::close);
				} catch (Exception ignored) {
				}
				try {
					Optional.ofNullable(mongoClient).ifPresent(MongoClient::close);
				} catch (Exception ignored) {
				}
			}
		});
		ttlThread.start();
		ttlThreadMap.put(rb.getName(), ttlThread);
		return this;
	}

	private boolean ttlIsRunning() {
		return !Thread.currentThread().isInterrupted();
	}

	public long findSequence(Ringbuffer<Document> rb, long timestamp) {
		if (this.ringBufferStorageMode == StorageMode.MongoDB) {
			try (MongoClient mongoClient = new MongoClient(new MongoClientURI(this.ringBufferMongoUri))) {
				MongoCollection<Document> cacheCollection = mongoClient.getDatabase(this.ringBufferDB).getCollection(this.ringBufferCollection);
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

		if (this.ringBufferStorageMode == StorageMode.RocksDB) {
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
