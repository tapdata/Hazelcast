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
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
			String sKey = sign + key;
			this.rocksDBResource.getRocksDB().delete(sKey.getBytes(StandardCharsets.UTF_8));
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
			String sKey = sign + key;
			Document val = ((Document) value).append("_ts", System.currentTimeMillis() / 1000);
			BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
			documentCodec.encode(writer, val, encoderContext);
			this.rocksDBResource.getRocksDB().put(sKey.getBytes(StandardCharsets.UTF_8), outputBuffer.toByteArray());
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
		String sKey = sign + key;
		try {
			byte[] s = this.rocksDBResource.getRocksDB().get(sKey.getBytes(StandardCharsets.UTF_8));
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
		if (!checkEnable() || rocksDBResource == null) {
			return true;
		}

		try (RocksIterator iterator = rocksDBResource.getRocksDB().newIterator()) {
			// Seek to the first key with our sign prefix
			byte[] signBytes = sign.getBytes(StandardCharsets.UTF_8);
			iterator.seek(signBytes);

			// Check if we found any key that starts with our sign
			if (iterator.isValid()) {
				byte[] key = iterator.key();
				if (key != null && key.length >= signBytes.length) {
					// Check if the key starts with our sign prefix
					for (int i = 0; i < signBytes.length; i++) {
						if (key[i] != signBytes[i]) {
							return true; // No keys with our prefix found
						}
					}
					return false; // Found at least one key with our prefix
				}
			}
			return true; // No valid keys found
		} catch (Exception e) {
			throw new RuntimeException("Failed to check if RocksDB IMap[" + imapName + "] is empty", e);
		}
	}
}