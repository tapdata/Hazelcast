package com.hazelcast.persistence;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author dayun
 * @Date 9/2/22
 */
public class PersistenceStorageManager {
    private Map<String, PersistenceStorage> persistenceStorageMap = new ConcurrentHashMap<>();

    private PersistenceStorageManager() {
    }

    public static PersistenceStorageManager getInstance() {
        return PersistenceStorageManager.PersistenceStorageManagerSingleton.INSTANCE.getInstance();
    }

    private enum PersistenceStorageManagerSingleton {
        INSTANCE;

        private final PersistenceStorageManager persistenceStorageManager;

        public PersistenceStorageManager getInstance() {
            return persistenceStorageManager;
        }

        PersistenceStorageManagerSingleton() {
            this.persistenceStorageManager = new PersistenceStorageManager();
        }
    }

    public void addStorage(final String storageConfigName, final PersistenceStorage persistenceStorage) {
        this.persistenceStorageMap.put(storageConfigName, persistenceStorage);
    }

    public PersistenceStorage getStorage(final String storageConfigName) {
        return persistenceStorageMap.get(storageConfigName);
    }
}
