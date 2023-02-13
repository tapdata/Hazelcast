package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.external.impl.RocksDBResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
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
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class RocksDBRingBuffer extends PersistenceRingBufferStore<PersistenceRocksDBConfig, RocksDBResource> {
	private static final String keySplit = "__0x1__";
	private String sign;
	private Long largestSequence = -1L;
	private Long smallestSequence = 0L;
	private final String largestSequenceKey = "largestSequence";
	private final String smallestSequenceKey = "smallestSequence";
	private static Codec<Document> DOCUMENT_CODEC = new DocumentCodec();
	private RocksDBResource rocksDBResource;
	private PersistenceRocksDBConfig persistenceRocksDBConfig;

	static {
		RocksDB.loadLibrary();
	}

	public RocksDBRingBuffer() {
	}

	@Override
	public void doInit(PersistenceRocksDBConfig persistenceRocksDBConfig, RocksDBResource rocksDBResource) {
		super.doInit(persistenceRocksDBConfig, rocksDBResource);
		this.rocksDBResource = rocksDBResource;
		this.persistenceRocksDBConfig = persistenceRocksDBConfig;
		this.sign = super.ringBufferName + keySplit;
		this.largestSequence = this._getLargestSequence();
		this.smallestSequence = this._getSmallestSequence();
	}

	@Override
	public void doDestroy() {
		Optional.ofNullable(this.rocksDBResource).ifPresent(rr -> CommonUtils.handleWithError(
				() -> {
					rr.close();
					this.rocksDBResource = null;
				}, throwable -> {
					throw new RuntimeException("Close RingBuffer[" + ringBufferName + "]'s RocksDB resource failed, config: " + persistenceRocksDBConfig, throwable);
				})
		);
	}

	@Override
	public void store(long sequence, Document value) {
		value = value.append("_ts", System.currentTimeMillis() / 1000);
		String key = sign + sequence;
		try (
				BasicOutputBuffer outputBuffer = new BasicOutputBuffer()
		) {
			BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
			DOCUMENT_CODEC.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
			this.rocksDBResource.getRocksDB().put(key.getBytes(StandardCharsets.UTF_8), outputBuffer.toByteArray());
			if (sequence <= largestSequence) {
				return;
			}
			largestSequence = sequence;
			key = sign + largestSequenceKey;
			this.rocksDBResource.getRocksDB().put(key.getBytes(StandardCharsets.UTF_8), ((Long) sequence).toString().getBytes());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void storeAll(long sequence, Document[] values) {
		for (Document value : values) {
			store(sequence, value);
			sequence = sequence + 1;
		}
	}

	@Override
	public Document load(long sequence) {
		String key = sign + sequence;
		Document doc;
		try {
			byte[] s = this.rocksDBResource.getRocksDB().get(key.getBytes(StandardCharsets.UTF_8));
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

	@Override
	public void delete(long s) {
		try {
			this.rocksDBResource.getRocksDB().delete((sign + s).getBytes(StandardCharsets.UTF_8));
			this.rocksDBResource.getRocksDB().put((sign + "smallestSequence").getBytes(StandardCharsets.UTF_8), ((Long) (s + 1)).toString().getBytes());
		} catch (RocksDBException e) {
			throw new RuntimeException("Delete from rocksdb failed, key: " + sign + s, e);
		}
	}

	@Override
	public long getLargestSequence() {
		return largestSequence;
	}

	public long _getLargestSequence() {
		String key = sign + largestSequenceKey;
		try {
			byte[] s = this.rocksDBResource.getRocksDB().get(key.getBytes(StandardCharsets.UTF_8));
			if (s == null) {
				return -1L;
			}
			String l = new String(s);
			return Long.parseLong(l);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long getSmallestSequence() {
		return this._getSmallestSequence();
	}

	public long _getSmallestSequence() {
		String key = sign + smallestSequenceKey;
		try {
			byte[] s = this.rocksDBResource.getRocksDB().get(key.getBytes(StandardCharsets.UTF_8));
			if (s == null) {
				return 0;
			}
			String l = new String(s);
			return Long.parseLong(l);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}
}