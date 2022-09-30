package com.hazelcast.persistence;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.rocksdb.*;
import org.bson.Document;

import javax.print.Doc;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

public class RocksDBIMap implements MapStore<String, Object>, MapLoaderLifecycleSupport {
    private static final String defaultDBPath   = "." + File.separator + "imap-cache-data" + File.separator;
    private static final String keySplit = "__0x0__";
    private String imapName;
    private String sign;
    private RocksDB rocksDB;
    private static Codec<Document> DOCUMENT_CODEC = new DocumentCodec();
    static {
        RocksDB.loadLibrary();
    }
    public RocksDBIMap() {
    }

    public synchronized void delete(String key) {
        try {
            String sKey = sign + key;
            rocksDB.delete(sKey.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public synchronized void store(String key, Object value) throws RuntimeException {
        if (! (value instanceof Document)) {
            return;
        }
        try {
            String sKey = sign + key;
            Document val = ((Document) value).append("_ts", System.currentTimeMillis()/1000);
            BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
            BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
            DOCUMENT_CODEC.encode(writer, val, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            rocksDB.put(sKey.getBytes(), outputBuffer.toByteArray());
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

    public synchronized Document load(String key) throws RuntimeException {
        Document doc;
        String sKey = sign + key;
        try {
            byte[] s = rocksDB.get(sKey.getBytes());
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

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String imapName) throws RuntimeException{
        String dbPath = properties.getProperty("rocksdb.dbPath");
        if (dbPath == null) {
            dbPath = defaultDBPath;
        }
        this.rocksDB = RocksDBInstance.getInstance(dbPath);
        this.imapName = imapName;
        this.sign = imapName + keySplit;
    }

    @Override
    public void destroy() {
        this.rocksDB.close();
    }
}