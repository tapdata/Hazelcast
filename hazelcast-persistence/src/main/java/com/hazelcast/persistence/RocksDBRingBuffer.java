package com.hazelcast.persistence;

import com.hazelcast.ringbuffer.RingbufferStore;
import org.rocksdb.*;
import org.bson.Document;

import java.util.Properties;

public class RocksDBRingBuffer implements RingbufferStore<Document> {
    private static final String defaultDBPath   = "./imap-cache-data/";
    private RocksDB rocksDB;
    private String ringBufferName;
    private static final String keySplit = "__0x1__";
    private String sign;
    private Long largestSequence = 0L;
    private final String largestSequenceKey = "largestSequence";
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
        final Options options = new Options().setCreateIfMissing(true);
        try {
            this.rocksDB = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
        this.ringBufferName = ringBufferName;
        this.sign = ringBufferName + keySplit;
    }

    @Override
    public void destroy() {
        rocksDB.close();
    }

    @Override
    public void store(long sequence, Document value) {
        String key = sign + sequence;
        try {
            rocksDB.put(key.getBytes(), value.toJson().getBytes());
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
            doc = Document.parse(new String(s));
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        } catch (RuntimeException e) {
            throw new RuntimeException(e.getMessage());
        }
        return doc;
    }

    @Override
    public long getLargestSequence() {
        String key = sign + largestSequenceKey;
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