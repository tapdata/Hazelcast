package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.external.impl.RocksDBResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RocksDBIMap extends PersistenceMapStore<PersistenceRocksDBConfig, RocksDBResource> {
	private static final String keySplit = "__0x0__";
	private String sign;
	private static Codec<Document> DOCUMENT_CODEC = new DocumentCodec();
	private RocksDBResource rocksDBResource;
	private PersistenceRocksDBConfig persistenceRocksDBConfig;

	static {
		RocksDB.loadLibrary();
	}

	public RocksDBIMap() {
	}

	@Override
	public void doInit(PersistenceRocksDBConfig persistenceRocksDBConfig, RocksDBResource rocksDBResource) throws RuntimeException {
		super.doInit(persistenceRocksDBConfig, rocksDBResource);
		this.rocksDBResource = rocksDBResource;
		this.persistenceRocksDBConfig = persistenceRocksDBConfig;
		this.sign = super.imapName + keySplit;
	}

	@Override
	public void doDestroy() {
		releaseResource();
	}

	@Override
	public void destroy() {
		// todo delete all data
		releaseResource();
	}

	private void releaseResource() {
		Optional.ofNullable(this.rocksDBResource).ifPresent(rr -> CommonUtils.handleWithError(
				() -> {
					rr.close();
					this.rocksDBResource = null;
				}, throwable -> {
					throw new RuntimeException("Close IMap[" + imapName + "]'s RocksDB resource failed, config: " + persistenceRocksDBConfig, throwable);
				}
		));
	}

	public synchronized void delete(String key) {
		try {
			String sKey = sign + key;
			this.rocksDBResource.getRocksDB().delete(sKey.getBytes());
		} catch (RocksDBException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public synchronized void store(String key, Object value) throws RuntimeException {
		if (!(value instanceof Document)) {
			return;
		}
		try (
				BasicOutputBuffer outputBuffer = new BasicOutputBuffer()
		) {
			String sKey = sign + key;
			Document val = ((Document) value).append("_ts", System.currentTimeMillis() / 1000);
			BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
			DOCUMENT_CODEC.encode(writer, val, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
			this.rocksDBResource.getRocksDB().put(sKey.getBytes(), outputBuffer.toByteArray());
		} catch (RocksDBException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public synchronized void storeAll(Map<String, Object> map) {
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			store(entry.getKey(), entry.getValue());
		}
	}

	public synchronized void deleteAll(Collection<String> keys) {
		for (String key : keys) {
			delete(key);
		}
	}

	public synchronized Document load(String key) {
		Document doc;
		String sKey = sign + key;
		try {
			byte[] s = this.rocksDBResource.getRocksDB().get(sKey.getBytes());
			if (s == null) {
				return null;
			}
			BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(s));
			doc = DOCUMENT_CODEC.decode(bsonReader, DecoderContext.builder().build());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return doc;
	}

	public synchronized Map<String, Object> loadAll(Collection<String> keys) {
		Map<String, Object> result = new HashMap<>();
		for (String key : keys) {
			result.put(key, load(key));
		}
		return result;
	}

	public Iterable<String> loadAllKeys() {
		return null;
	}
}