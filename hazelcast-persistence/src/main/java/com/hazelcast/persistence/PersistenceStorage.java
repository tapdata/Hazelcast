package com.hazelcast.persistence;

import com.hazelcast.config.*;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;

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

	private PersistenceStorage initMapStoreConfig(Config c) {
		if (this.imapStorageMode == StorageMode.Mem) {
			return this;
		}
		MapConfig mapCfg = new MapConfig();
		mapCfg.setName("default");
		MapStoreConfig mapStoreCfg = new MapStoreConfig();
		switch (this.imapStorageMode) {
			case MongoDB:
				mapStoreCfg.setClassName(MonogoDBIMap.class.getName())
						.setProperty("mongo.uri", this.imapMongoUri)
						.setProperty("mongo.db", this.imapDB)
						.setProperty("mongo.collection", this.imapCollection);
				break;
			case RocksDB:
				mapStoreCfg.setClassName(RocksDBIMap.class.getName())
						.setProperty("rocksdb.dbPath", this.imapRocksDBPath);
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

	private PersistenceStorage initRingBufferConfig(Config c) {
		if (this.ringBufferStorageMode == StorageMode.Mem) {
			return this;
		}

		RingbufferConfig ringbufferConfig = new RingbufferConfig();
		ringbufferConfig.setName("default").setCapacity(this.ringBufferInMemSize);
		RingbufferStoreConfig ringbufferStoreConfig = new RingbufferStoreConfig();
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
		new Thread(() -> {
			long sleepSeconds = 60;
			if (ttlSeconds < 60) {
				sleepSeconds = ttlSeconds;
			}
			if (sleepSeconds < 10) {
				sleepSeconds = 10;
			}
			sleepSeconds = 1;
			RocksDB rocksDB = null;
			MongoCollection<Document> cacheCollection = null;
			String keySplit = "__0x1__";
			String sign = rb.getName() + keySplit;
			if (this.ringBufferStorageMode == StorageMode.RocksDB) {
				rocksDB = RocksDBInstance.getInstance(this.ringBufferRocksDBPath);
			}
			if (this.ringBufferStorageMode == StorageMode.MongoDB) {
				MongoClient mongoClient = new MongoClient(new MongoClientURI(this.ringBufferMongoUri));
				cacheCollection = mongoClient.getDatabase(this.ringBufferDB).getCollection(this.ringBufferCollection);
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
						if (this.ringBufferStorageMode == StorageMode.RocksDB) {
							try {
								rocksDB.delete((sign + s).getBytes(StandardCharsets.UTF_8));
								rocksDB.put((sign + "smallestSequence").getBytes(StandardCharsets.UTF_8), ((Long) (s + 1)).toString().getBytes());
							} catch (RocksDBException e) {
							}
						}
						if (this.ringBufferStorageMode == StorageMode.MongoDB) {
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
}
