package com.hazelcast.persistence;

public enum StorageMode {
    MongoDB,
    RocksDB,
    DuckDB,
    Mem,
    HTTP_TM,
}
