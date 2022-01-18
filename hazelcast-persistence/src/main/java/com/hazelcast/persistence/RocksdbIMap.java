package com.hazelcast.persistence;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import org.rocksdb.*;
import org.bson.Document;

import java.util.*;

public class RocksdbIMap implements MapStore<String, Document>, MapLoaderLifecycleSupport {
    private static final String defaultDBPath   = "./imap-cache-data/";
    private static final String keySplit = "__0x0__";
    private String imapName;
    private String sign;
    private RocksDB rocksDB;
    static {
        RocksDB.loadLibrary();
    }
    public RocksdbIMap() {
    }

    public synchronized void delete(String key) {
        try {
            String sKey = sign + key;
            rocksDB.delete(sKey.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public synchronized void store(String key, Document value) throws RuntimeException {
        try {
            String sKey = sign + key;
            rocksDB.put(sKey.getBytes(), value.toJson().getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public synchronized void storeAll(Map<String, Document> map) {
        for (Map.Entry<String, Document> entry : map.entrySet()) {
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
            doc = Document.parse(new String(s));
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        } catch (RuntimeException e) {
            throw new RuntimeException(e.getMessage());
        }
        return doc;
    }

    public synchronized Map<String, Document> loadAll(Collection<String> keys) {
        Map<String, Document> result = new HashMap<>();
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
        final Options options = new Options().setCreateIfMissing(true);
        try {
            this.rocksDB = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException(e.getMessage());
        }
        this.imapName = imapName;
        this.sign = imapName + keySplit;
    }

    @Override
    public void destroy() {
        this.rocksDB.close();
    }
}