package com.hazelcast.persistence;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBInstance {
    private String path = "";
    private static RocksDB rocksDBInstance = null;
    private RocksDBInstance() {}
    public static synchronized RocksDB getInstance(String path) {
        if (rocksDBInstance == null) {
            final Options options = new Options().setCreateIfMissing(true);
            try {
                rocksDBInstance = RocksDB.open(options, path);
            } catch (RocksDBException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        return rocksDBInstance;
    }
}