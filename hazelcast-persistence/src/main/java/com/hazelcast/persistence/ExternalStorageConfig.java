package com.hazelcast.persistence;

import com.hazelcast.config.Config;

import java.util.Date;
import java.util.List;

/**
 * @Author dayun
 * @Date 9/2/22
 */
public class ExternalStorageConfig<T> {
    private String configName;
    private StorageMode storageMode;
    private T config;

    private List<String> relatedTaskIds;

    public Date getCreateTime() {
        return createTime;
    }

    private final Date createTime = new Date();

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public StorageMode getStorageMode() {
        return storageMode;
    }

    public void setStorageMode(StorageMode storageMode) {
        this.storageMode = storageMode;
    }

    public T getConfig() {
        return config;
    }

    public void setConfig(T config) {
        this.config = config;
    }

    public boolean checkIfValid() {
        return (this.config instanceof MongoDBConfig && StorageMode.MongoDB.equals(this.storageMode))
                || (this.config instanceof RocksDBConfig && StorageMode.RocksDB.equals(this.storageMode));
    }
}
