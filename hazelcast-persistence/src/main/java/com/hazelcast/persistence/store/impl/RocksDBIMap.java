package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.resource.impl.RocksDBResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RocksDBIMap extends PersistenceMapStore<PersistenceRocksDBConfig, RocksDBResource> {
	private static final String keySplit = "__0x0__";
	private String sign;
	private static Codec<Document> documentCodec;
	private static EncoderContext encoderContext;
	private RocksDBResource rocksDBResource;
	private PersistenceRocksDBConfig persistenceRocksDBConfig;

	static {
		documentCodec = new DocumentCodec(MongodbUtil.getForJavaCodecRegistry());
		encoderContext = EncoderContext.builder()
				.isEncodingCollectibleDocument(true)
				.build();
	}

	private DecoderContext decoderContext;

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

	@Override
	public void doClear() {
		ColumnFamilyHandle cfHandle = this.rocksDBResource.getColumnFamilyHandle();
		try {
			this.rocksDBResource.getRocksDB().dropColumnFamilies(Collections.singletonList(cfHandle));
			this.rocksDBResource.removeColumnFamily();
		}catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
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
		if (!checkEnable()) {
			return;
		}
		try {
			ColumnFamilyHandle cfHandle = this.rocksDBResource.getColumnFamilyHandle();
			this.rocksDBResource.getRocksDB().delete(cfHandle, key.getBytes(StandardCharsets.UTF_8));
		} catch (RocksDBException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public synchronized void store(String key, Object value) throws RuntimeException {
		if (!checkEnable()) {
			return;
		}
		if (!(value instanceof Document)) {
			return;
		}
		try (
				BasicOutputBuffer outputBuffer = new BasicOutputBuffer()
		) {
			Document val = ((Document) value).append("_ts", System.currentTimeMillis() / 1000);
			BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
			documentCodec.encode(writer, val, encoderContext);
			ColumnFamilyHandle cfHandle = this.rocksDBResource.getColumnFamilyHandle();
			this.rocksDBResource.getRocksDB().put(cfHandle, key.getBytes(StandardCharsets.UTF_8), outputBuffer.toByteArray());
		} catch (RocksDBException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public synchronized void storeAll(Map<String, Object> map) {
		if (!checkEnable()) {
			return;
		}
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			store(entry.getKey(), entry.getValue());
		}
	}

	public synchronized void deleteAll(Collection<String> keys) {
		if (!checkEnable()) {
			return;
		}
		for (String key : keys) {
			delete(key);
		}
	}

	public synchronized Document load(String key) {
		Document doc;
		try {
			ColumnFamilyHandle cfHandle = this.rocksDBResource.getColumnFamilyHandle();
			byte[] s = this.rocksDBResource.getRocksDB().get(cfHandle, key.getBytes(StandardCharsets.UTF_8));
			if (s == null) {
				return null;
			}
			BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(s));
			decoderContext = DecoderContext.builder().build();
			doc = documentCodec.decode(bsonReader, decoderContext);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return doc;
	}

	public synchronized Map<String, Object> loadAll(Collection<String> keys) {
		Map<String, Object> result = new HashMap<>();
		for (String key : keys) {
			if (null == key) {
				continue;
			}
			Document value = load(key);
			if (null == value) {
				continue;
			}
			result.put(key, value);
		}
		return result;
	}

	public Iterable<String> loadAllKeys() {
		return null;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public synchronized Map<String,Object> getStatistics() {
		if (!checkEnable()) {
			return null;
		}
		try {
			long count = this.rocksDBResource.getRocksDB().getLongProperty(this.rocksDBResource.getColumnFamilyHandle(), "rocksdb.estimate-num-keys");
			long size = this.rocksDBResource.getRocksDB().getLongProperty(this.rocksDBResource.getColumnFamilyHandle(), "rocksdb.estimate-live-data-size");
			Map<String, Object> statistics = new HashMap<>();
			statistics.put("count", count);
			statistics.put("uri", this.rocksDBResource.getDbPath() + File.separator + this.rocksDBResource.getColumnFamilyName());
			statistics.put("mode", persistenceRocksDBConfig.getStorageMode().name());
			statistics.put("size", size);
			return statistics;
		} catch (Exception e) {
			throw new RuntimeException(e);
        }
	}

}