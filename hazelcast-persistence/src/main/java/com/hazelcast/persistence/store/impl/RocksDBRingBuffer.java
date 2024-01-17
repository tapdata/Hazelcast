package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.MongodbUtil;
import com.hazelcast.persistence.config.PersistenceRocksDBConfig;
import com.hazelcast.persistence.resource.impl.RocksDBResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBRingBuffer extends PersistenceRingBufferStore<PersistenceRocksDBConfig, RocksDBResource> {
	private static final String keySplit = "__0x1__";
	private String sign;
	private Long largestSequence = -1L;
	private Long smallestSequence = 0L;
	private final String largestSequenceKey = "largestSequence";
	private final String smallestSequenceKey = "smallestSequence";
	private static Codec<Document> documentCodec;
	private static EncoderContext encoderContext;
	private RocksDBResource rocksDBResource;
	private PersistenceRocksDBConfig persistenceRocksDBConfig;

	private static Map<String, Long> sequenceMap = new ConcurrentHashMap<>();
	static {
		documentCodec = new DocumentCodec(MongodbUtil.getForJavaCodecRegistry());
		encoderContext = EncoderContext.builder()
				.isEncodingCollectibleDocument(true)
				.build();
	}

	public RocksDBRingBuffer() {
	}

	@Override
	public void doInit(PersistenceRocksDBConfig persistenceRocksDBConfig, RocksDBResource rocksDBResource) {
		super.doInit(persistenceRocksDBConfig, rocksDBResource);
		this.rocksDBResource = rocksDBResource;
		this.persistenceRocksDBConfig = persistenceRocksDBConfig;
		this.sign = super.ringBufferName + keySplit;
		flushSequence();
	}

	private void flushSequence() {
		this.largestSequence = this._getLargestSequence();
		this.smallestSequence = this._getSmallestSequence();
		String smallKey = sign + smallestSequenceKey;
		String largeKey = sign + largestSequenceKey;
		sequenceMap.put(smallKey,smallestSequence);
		sequenceMap.put(largeKey,largestSequence);

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
	public void store(long sequence, Object value) {
		if (!(value instanceof Document)) {
			return;
		}
		Document document = (Document) value;
		if (!checkEnable()) {
			return;
		}
		value = document.append("_ts", System.currentTimeMillis() / 1000);
		String key = sign + sequence;
		try (
				BasicOutputBuffer outputBuffer = new BasicOutputBuffer()
		) {
			BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
			documentCodec.encode(writer, document, encoderContext);
			this.rocksDBResource.getRocksDB().put(key.getBytes(StandardCharsets.UTF_8), outputBuffer.toByteArray());
			if (sequence <= largestSequence) {
				return;
			}
			largestSequence = sequence;
			key = sign + largestSequenceKey;
			sequenceMap.put(key,largestSequence);
			this.rocksDBResource.getRocksDB().put(key.getBytes(StandardCharsets.UTF_8), ((Long) sequence).toString().getBytes());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void storeAll(long sequence, Object[] values) {
		if (!checkEnable()) {
			return;
		}
		for (Object value : values) {
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
			doc = documentCodec.decode(bsonReader, DecoderContext.builder().build());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return doc;
	}

	@Override
	public void delete(long s) {
		if (!checkEnable()) {
			return;
		}
		try {
			this.rocksDBResource.getRocksDB().delete((sign + s).getBytes(StandardCharsets.UTF_8));
			this.rocksDBResource.getRocksDB().put((sign + "smallestSequence").getBytes(StandardCharsets.UTF_8), ((Long) (s + 1)).toString().getBytes());
		} catch (RocksDBException e) {
			throw new RuntimeException("Delete from rocksdb failed, key: " + sign + s, e);
		}
	}

	@Override
	public long getLargestSequence() {
		return sequenceMap.get(sign + largestSequenceKey);
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

	@Override
	public long findSequenceByTimestamp(long timestamp) {
		flushSequence();
		if (largestSequence == -1) {
			return 0;
		}
		for (long i = smallestSequence; i <= largestSequence; i++) {
			try {
				Document document = load(i);
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
		return largestSequence + 1L;
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