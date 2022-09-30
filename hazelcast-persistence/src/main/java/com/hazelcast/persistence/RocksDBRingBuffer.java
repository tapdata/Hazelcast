package com.hazelcast.persistence;

import com.hazelcast.ringbuffer.RingbufferStore;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.rocksdb.*;
import org.bson.Document;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;

public class RocksDBRingBuffer implements RingbufferStore<Document> {
	private static final String defaultDBPath   = "." + File.separator + "imap-cache-data" + File.separator;
    private RocksDB rocksDB;
    private String ringBufferName;
    private static final String keySplit = "__0x1__";
    private String sign;
    private Long largestSequence = -1L;
    private Long smallestSequence = 0L;
    private final String largestSequenceKey = "largestSequence";
    private final String smallestSequenceKey = "smallestSequence";
    private static Codec<Document> DOCUMENT_CODEC = new DocumentCodec();
    static {
        RocksDB.loadLibrary();
    }
    public RocksDBRingBuffer() {
    }
    @Override
    public void init(Properties properties, String ringBufferName) {
        String dbPath = properties.getProperty("rocksdb.dbPath");
        if (dbPath == null) {
            dbPath = defaultDBPath;
        }
        this.rocksDB = RocksDBInstance.getInstance(dbPath);
        this.ringBufferName = ringBufferName;
        this.sign = ringBufferName + keySplit;
        this.largestSequence = this._getLargestSequence();
        this.smallestSequence = this._getSmallestSequence();

    }

    @Override
    public void destroy() {
        rocksDB.close();
    }

    @Override
    public void store(long sequence, Document value) {
        value = value.append("_ts", System.currentTimeMillis()/1000);
        String key = sign + sequence;
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        DOCUMENT_CODEC.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        try {
            rocksDB.put(key.getBytes(), outputBuffer.toByteArray());
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
        if (sequence <= largestSequence) {
            return;
        }
        largestSequence = sequence;
        key = sign + largestSequenceKey;
        try {
            rocksDB.put(key.getBytes(), ((Long) sequence).toString().getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
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
            byte[] s = rocksDB.get(key.getBytes());
            if (s == null) {
                return null;
            }
            BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(s));
            doc = DOCUMENT_CODEC.decode(bsonReader, DecoderContext.builder().build());
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        } catch (RuntimeException e) {
            throw new RuntimeException(e.getMessage());
        }
        return doc;
    }

    @Override
    public long getLargestSequence() {
        return largestSequence;
    }

    public long _getLargestSequence() {
        String key = sign + largestSequenceKey;
        try {
            byte[] s = rocksDB.get(key.getBytes());
            if (s == null) {
                return -1L;
            }
            String l = new String(s);
            return Long.parseLong(l);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public long getSmallestSequence() {
        return this._getSmallestSequence();
    }

    public long _getSmallestSequence() {
        String key = sign + smallestSequenceKey;
        try {
            byte[] s = rocksDB.get(key.getBytes());
            if (s == null) {
                return 0;
            }
            String l = new String(s);
            return Long.parseLong(l);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}