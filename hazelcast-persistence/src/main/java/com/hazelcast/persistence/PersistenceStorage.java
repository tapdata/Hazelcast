package com.hazelcast.persistence;

import com.hazelcast.config.*;
import com.hazelcast.map.IMap;
import com.hazelcast.persistence.config.HazelcastStoreConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.ExternalResource;
import com.hazelcast.persistence.resource.ExternalResourceFactory;
import com.hazelcast.persistence.store.*;
import com.hazelcast.persistence.store.ttl.TTLCleanMode;
import com.hazelcast.persistence.store.ttl.TTLCleanRuleBase;
import com.hazelcast.persistence.store.ttl.TTLService;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PersistenceStorage {
    private final ConcurrentHashMap<String, Thread> ttlThreadMap = new ConcurrentHashMap<>();
    private Logger logger = LogManager.getLogger(PersistenceStorage.class);
    private final ConcurrentHashMap<String, PersistenceStorageAbstractConfig> persistenceConfigMap = new ConcurrentHashMap<>();
    private final MultiReferenceMap<String, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>> storeImplementationMap = new MultiReferenceMap<String, PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>>() {
        @Override
        protected void destroyValue(PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> value) {
            value.doDestroy();
        }
    };

    private PersistenceStorage() {
    }

    private static final PersistenceStorage persistenceStorage = new PersistenceStorage();

    public static PersistenceStorage getInstance() {
        return persistenceStorage;
    }

    public static String getConfigKey(ConstructType constructType, String name) {
        if (null == constructType) {
            throw new IllegalArgumentException("Construct type cannot be null");
        }
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Name cannot be blank");
        }
        return String.join("-", constructType.name(), name);
    }

    public PersistenceStorageAbstractConfig getPersistenceStorageConfig(ConstructType constructType, String name) {
        String configKey = getConfigKey(constructType, name);
        return persistenceConfigMap.get(configKey);
    }

    public synchronized PersistenceStorage addConfig(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
        String configKey;
        try {
            configKey = getConfigKey(persistenceStorageAbstractConfig.getConstructType(), persistenceStorageAbstractConfig.getName());
        } catch (Exception e) {
            throw new RuntimeException("Get config key failed", e);
        }
        PersistenceStorageAbstractConfig existingConfig = persistenceConfigMap.get(configKey);
        if (null != existingConfig) {
            if (!existingConfig.getStorageMode().equals(persistenceStorageAbstractConfig.getStorageMode())) {
                // Nonsupport change storage mode
                throw new RuntimeException("Change persistence storage mode is not allowed until restart instance\n old: " + existingConfig + "\n new: " + persistenceStorageAbstractConfig);
            }
        }
        persistenceConfigMap.put(configKey, persistenceStorageAbstractConfig);
        return this;
    }

    public PersistenceStorage logger(Logger logger) {
        this.logger = logger;
        return this;
    }

    public PersistenceStorage initMapStoreConfig(Config c) {
        return initMapStoreConfig(c, "default");
    }

    public PersistenceStorage initMapStoreConfig(Config c, String mapName) {
        return initMapStoreConfig(MultiReferenceMap.DEFAULT_REFERENCE_ID, c, mapName);
    }

    public PersistenceStorage initMapStoreConfig(String referenceId, Config c, String mapName) {
        //checkInitConfig(mapName, ConstructType.IMAP);
        PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(ConstructType.IMAP, mapName);
        if (null == persistenceStorageAbstractConfig) {
            throw new IllegalArgumentException(String.format("IMap name %s's persistence storage config is not exists, please add config", mapName));
        }
        StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
        MapConfig mapCfg = c.getMapConfig(mapName);
        MapStoreConfig mapStoreCfg = mapCfg.getMapStoreConfig();

        ExternalResource<PersistenceStorageAbstractConfig> externalResource = getExternalResource(storageMode);
        if (null == externalResource) {
            return this;
        }
        boolean initResult;
        try {
            HazelcastStoreConfig<MapStoreConfig> hazelcastStoreConfig = new HazelcastStoreConfig<>(mapStoreCfg);
            initResult = initStore(
                    ConstructType.IMAP,
                    referenceId,
                    mapName,
                    persistenceStorageAbstractConfig,
                    externalResource,
                    hazelcastStoreConfig
            );
        } catch (Exception e) {
            CommonUtils.ignoreAnyError(externalResource::close);
            throw new RuntimeException(e);
        }
        String maxSizePolicy = persistenceStorageAbstractConfig.getMaxSizePolicy();
        MaxSizePolicy maxSizePolicyEnum = MaxSizePolicy.PER_NODE;
        if (StringUtils.isNotEmpty(maxSizePolicy)) {
            try {
                maxSizePolicyEnum = MaxSizePolicy.valueOf(maxSizePolicy);
            } catch (IllegalArgumentException e) {
                logger.warn("Max size policy {} is not supported, use default {}", maxSizePolicy, maxSizePolicyEnum);
            }
        }
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(maxSizePolicyEnum)
                .setSize(persistenceStorageAbstractConfig.getInMemSize());
        mapCfg.setEvictionConfig(evictionConfig);
        if (initResult) {
            mapStoreCfg.setEnabled(true)
                    .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY)
                    .setWriteDelaySeconds(persistenceStorageAbstractConfig.getWriteDelaySeconds());
            mapCfg.setMapStoreConfig(mapStoreCfg);
            mapCfg.setDataPersistenceConfig(new DataPersistenceConfig().setEnabled(true));
        }
        c.addMapConfig(mapCfg);
        return this;
    }

    public void clear(ConstructType constructType, String name) {
        PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = storeImplementationMap.get(getConfigKey(constructType, name));
        if (null != store) {
            store.doClear();
        }
    }

    @Deprecated
    public void destroy(String name) {
        CommonUtils.ignoreAnyError(() -> storeImplementationMap.destroy(MultiReferenceMap.DEFAULT_REFERENCE_ID, name));
    }

    public void destroy(ConstructType constructType, String name) {
        destroy(MultiReferenceMap.DEFAULT_REFERENCE_ID, constructType, name);
    }

    public boolean destroy(String referenceId, ConstructType constructType, String name) {
        AtomicBoolean removed = new AtomicBoolean(false);
        String configKey = getConfigKey(constructType, name);
        CommonUtils.ignoreAnyError(() -> removed.set(storeImplementationMap.destroy(referenceId, configKey)));
        if (removed.get()) {
            Optional.ofNullable(storeImplementationMap.get(configKey)).ifPresent(PersistenceStorageStore::disable);
            Optional.ofNullable(ttlService).ifPresent(ts -> ts.removeTTL(persistenceConfigMap.get(configKey)));
        }
        return removed.get();
    }

    public PersistenceStorage initRingBufferConfig(Config c) {
        return initRingBufferConfig(c, "default");
    }

    public PersistenceStorage initRingBufferConfig(Config c, String ringBufferName) {
        return initRingBufferConfig(MultiReferenceMap.DEFAULT_REFERENCE_ID, c, ringBufferName);
    }

    public PersistenceStorage initRingBufferConfig(String referenceId, Config c, String ringBufferName) {
        return initRingBufferConfig(referenceId, c, ringBufferName, null);
    }

    public PersistenceStorage initRingBufferConfig(String referenceId, Config c, String ringBufferName, SequenceMode sequenceMode) {
        checkInitConfig(ringBufferName, ConstructType.RINGBUFFER);
        sequenceMode = (null == sequenceMode) ? SequenceMode.STORE : sequenceMode;
        PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(ConstructType.RINGBUFFER, ringBufferName);
        if (null == persistenceStorageAbstractConfig) {
            throw new IllegalArgumentException(String.format("Ring buffer name %s's persistence storage config is not exists, please add config", ringBufferName));
        }
        persistenceStorageAbstractConfig.sequenceMode(sequenceMode);
        StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
        RingbufferConfig ringbufferConfig = c.getRingbufferConfig(ringBufferName);
        RingbufferStoreConfig ringbufferStoreConfig = ringbufferConfig.getRingbufferStoreConfig();

        ExternalResource<PersistenceStorageAbstractConfig> externalResource = getExternalResource(storageMode);
        if (null == externalResource) {
            return this;
        }
        boolean initResult;
        try {
            HazelcastStoreConfig<RingbufferStoreConfig> hazelcastStoreConfig = new HazelcastStoreConfig<>(ringbufferStoreConfig);
            initResult = initStore(
                    ConstructType.RINGBUFFER,
                    referenceId,
                    ringBufferName,
                    persistenceStorageAbstractConfig,
                    externalResource,
                    hazelcastStoreConfig
            );
        } catch (Exception e) {
            CommonUtils.ignoreAnyError(externalResource::close);
            throw new RuntimeException(e);
        }
        ringbufferConfig.setCapacity(persistenceStorageAbstractConfig.getInMemSize())
                .setInMemoryFormat(InMemoryFormat.OBJECT);
        if (initResult) {
            ringbufferStoreConfig.setEnabled(true);
            String sequenceModeStr = sequenceMode.name();
            ringbufferStoreConfig.setProperty("sequence-mode", sequenceModeStr);
            ringbufferConfig.setRingbufferStoreConfig(ringbufferStoreConfig);
        }
        c.addRingBufferConfig(ringbufferConfig);
        return this;
    }

    private synchronized boolean initStore(ConstructType constructType,
                                           String referenceId,
                                           String name,
                                           PersistenceStorageAbstractConfig persistenceStorageAbstractConfig,
                                           ExternalResource<PersistenceStorageAbstractConfig> externalResource,
                                           HazelcastStoreConfig<?> hazelcastStoreConfig) {
        String configKey = getConfigKey(constructType, name);
        StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
        if (storageMode == StorageMode.Mem && storeImplementationMap.containsKey(configKey)) {
            storeImplementationMap.get(configKey).disable();
        }
        if (storeImplementationMap.containsKey(configKey)) {
            PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = storeImplementationMap.get(configKey);
            if (!store.checkEnable()) {
                externalResource.doInit(persistenceStorageAbstractConfig);
                store.doInit(persistenceStorageAbstractConfig, externalResource);
            } else {
                store.lightInit();
            }
            store.enable();
            storeImplementationMap.addReference(referenceId, configKey);
        } else {
            PersistenceStoreFactory persistenceStoreFactory = new PersistenceStoreFactory();
            externalResource.doInit(persistenceStorageAbstractConfig);
            PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = persistenceStoreFactory.createStore(
                    constructType,
                    persistenceStorageAbstractConfig.getStorageMode()
            );
            if (null == store) {
                return false;
            }
            store.doInit(persistenceStorageAbstractConfig, externalResource);
            hazelcastStoreConfig.implementation(store);
            if (null == referenceId) {
                throw new RuntimeException("Reference id can not be null");
            }
            storeImplementationMap.init(referenceId, configKey, store);
        }
        return true;
    }

    private static void checkInitConfig(String name, ConstructType constructType) {
        if (StringUtils.isBlank(name) || "default".equals(name)) {
            throw new RuntimeException(String.format("Default %s config is not allowed", constructType.name()));
        }
    }

    private static ExternalResource<PersistenceStorageAbstractConfig> getExternalResource(StorageMode storageMode) {
        ExternalResourceFactory externalResourceFactory = new ExternalResourceFactory();
        return externalResourceFactory.createExternalResource(storageMode);
    }

    private static PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> getStore(StorageMode storageMode) {
        PersistenceStoreFactory persistenceStoreFactory = new PersistenceStoreFactory();
        return persistenceStoreFactory.createStore(ConstructType.RINGBUFFER, storageMode);
    }


    public PersistenceStorage initHZConfig(Config c) {
        this.initMapStoreConfig(c);
        this.initRingBufferConfig(c);
        return this;
    }

    public PersistenceStorage initHZConfig(Config c, String configName) {
        this.initMapStoreConfig(c, configName);
        this.initRingBufferConfig(c, configName);
        return this;
    }

    public PersistenceStorage setImapTTL(IMap<String, Object> imap, long ttlSeconds) {
        /*ConstructType constructType = ConstructType.IMAP;
        PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(constructType, imap.getName());
        StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
        if (storageMode == StorageMode.Mem || storageMode == StorageMode.HTTP_TM) {
            return this;
        }
        String ttlThreadKey = getTtlThreadKey(constructType, imap.getName());
        if (ttlThreadMap.containsKey(ttlThreadKey)) {
            // use thread's interrupt method to stop pre ttl thread
            ttlThreadMap.get(ttlThreadKey).interrupt();
        }
        Thread ttlThread = new Thread(() -> {
            Thread.currentThread().setName(String.format("Clear-IMap-TTL-%s", ttlThreadKey));
            long sleepSeconds = TimeUnit.HOURS.toSeconds(1L);
            PersistenceMapStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> persistenceMapStore = null;
            try {
                PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = createStore(persistenceStorageAbstractConfig);
                if (store instanceof PersistenceMapStore) {
                    persistenceMapStore = (PersistenceMapStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>) store;
                } else {
                    return;
                }
                while (ttlIsRunning()) {
                    try {
                        TimeUnit.SECONDS.sleep(sleepSeconds);
                    } catch (InterruptedException e) {
                        break;
                    }
                    try {
                        Iterator<Map.Entry<String, Object>> iterator = imap.iterator();
                        while (ttlIsRunning() && iterator.hasNext()) {
                            Map.Entry<String, Object> entry = iterator.next();
                            Object value = entry.getValue();
                            if (!(value instanceof Document)) {
                                continue;
                            }
                            Document document = (Document) entry.getValue();
                            Long _ts = getTs(document);
                            if (System.currentTimeMillis() - _ts * 1000 < ttlSeconds * 1000) {
                                break;
                            }
                            persistenceMapStore.delete(entry.getKey());
                        }
                    } catch (Exception e) {
                        if (null != logger) {
                            logger.warn("IMap [{}] clear ttl data failed, ttl seconds: {}", imap.getName(), ttlSeconds, e);
                        }
                    }
                }
            } finally {
                Optional.ofNullable(persistenceMapStore).ifPresent(PersistenceStorageStore::doDestroy);
            }
        });
        ttlThread.start();
        ttlThreadMap.put(ttlThreadKey, ttlThread);
        return this;*/
        return setTTL(ConstructType.IMAP, imap.getName(), ttlSeconds);
    }

    public PersistenceStorage setImapTTL(IMap<String, Object> imap,Long keyTTlSeconds ,Object condition,TTLCleanMode mode) throws Exception {
        if(condition.getClass().equals(mode.getCondition()) && keyTTlSeconds > 0){
            Class<?> clazz = Class.forName(mode.getTtlCleanRuleClazz());
            Constructor<?> constructor = clazz.getConstructor(Long.class,mode.getCondition());
            TTLCleanRuleBase ttlCleanRuleBase = (TTLCleanRuleBase) constructor.newInstance(keyTTlSeconds,condition);
            return setTTL(ConstructType.IMAP,imap.getName(),0,ttlCleanRuleBase);
        }
        logger.warn("Register ttl failed needClass:{},inputClass:{},keyTTlSeconds:{}",mode.getCondition(),condition.getClass(),keyTTlSeconds);
        return null;
    }

    public PersistenceStorage setRingBufferTTL(Ringbuffer<Document> rb, long ttlSeconds) {
        /*ConstructType constructType = ConstructType.RINGBUFFER;
        PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(constructType, rb.getName());
        StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
        if (storageMode == StorageMode.Mem || storageMode == StorageMode.HTTP_TM) {
            return this;
        }
        String ttlThreadKey = getTtlThreadKey(constructType, rb.getName());
        if (ttlThreadMap.containsKey(ttlThreadKey)) {
            // use thread's interrupt method to stop pre ttl thread
            ttlThreadMap.get(ttlThreadKey).interrupt();
        }
        Thread ttlThread = new Thread(() -> {
            Thread.currentThread().setName(String.format("Clear-RingBuffer-TTL-%s-%s-%s", storageMode.name(), rb.getName(), ttlSeconds));
            long sleepSeconds = 60;
            long ttlMillis = TimeUnit.SECONDS.toMillis(ttlSeconds);
            if (ttlSeconds < 60) {
                sleepSeconds = ttlSeconds;
            }
            if (sleepSeconds < 10) {
                sleepSeconds = 10;
            }
            PersistenceRingBufferStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> persistenceRingBufferStore = null;
            try {
                PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = createStore(persistenceStorageAbstractConfig);
                if (store instanceof PersistenceRingBufferStore) {
                    persistenceRingBufferStore = (PersistenceRingBufferStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>) store;
                } else {
                    return;
                }
                while (ttlIsRunning()) {
                    try {
                        TimeUnit.SECONDS.sleep(sleepSeconds);
                    } catch (InterruptedException e) {
                        break;
                    }
                    try {
                        if (rb.tailSequence() < 0) {
                            continue;
                        }
                        long s = rb.headSequence() - 1;
                        while (ttlIsRunning()) {
                            s++;
                            if (s >= rb.tailSequence()) {
                                break;
                            }
                            Document document = null;
                            try {
                                Object obj = persistenceRingBufferStore.load(s);
                                if (obj instanceof Document) {
                                    document = (Document) obj;
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("Read one from ringBuffer failed, sequence: " + s, e);
                            }
                            if (null == document) {
                                continue;
                            }
                            if (document.containsKey("type") && "SIGN".equals(document.getString("type"))) {
                                continue;
                            }
                            Long _ts = getTs(document);
                            if (_ts == null) continue;
                            if (System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(_ts) < ttlMillis) {
                                break;
                            }
                            persistenceRingBufferStore.delete(s);
                        }
                    } catch (Exception e) {
                        if (null != logger) {
                            logger.warn("Ringbuffer [{}] clear ttl data failed, ttl seconds: {}", rb.getName(), ttlSeconds, e);
                        }
                    }
                }
            } finally {
                Optional.ofNullable(persistenceRingBufferStore).ifPresent(PersistenceStorageStore::doDestroy);
            }
        });
        ttlThread.start();
        ttlThreadMap.put(ttlThreadKey, ttlThread);
        return this;*/
        return setTTL(ConstructType.RINGBUFFER, rb.getName(), ttlSeconds);
    }

    private static volatile TTLService ttlService;
    private PersistenceStorage setTTL(ConstructType constructType, String name, long ttlSeconds) {
        return setTTL(constructType,name,ttlSeconds,null);
    }

    private PersistenceStorage setTTL(ConstructType constructType, String name, long ttlSeconds,TTLCleanRuleBase ttlCleanRuleBase) {
        if (null == ttlService) {
            synchronized (TTLService.class) {
                if (null == ttlService) {
                    ttlService = new TTLService(this::createStore, logger);
                }
            }
        }
        PersistenceStorageAbstractConfig persistenceStorageConfig = getPersistenceStorageConfig(constructType, name);
        ttlService.registerTTL(persistenceStorageConfig,ttlSeconds,ttlCleanRuleBase);
        logger.info("Register ttl successfully,ttlRule:{},ttlSeconds:{}",ttlCleanRuleBase,ttlCleanRuleBase.getKeyTTLSeconds());
        ttlService.start();
        return this;
    }

    private static Long getTs(Document document) {
        if (null != document) {
            if (document.get("value") instanceof Document) {
                document = (Document) document.get("value");
            }

            if (document.containsKey("_ts")) {
                return document.getLong("_ts");
            }
        }

        return null;
    }

    private PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> createStore(
            PersistenceStorageAbstractConfig persistenceStorageAbstractConfig
    ) {
        if (null == persistenceStorageAbstractConfig) {
            return null;
        }
        ExternalResource<PersistenceStorageAbstractConfig> externalResource = new ExternalResourceFactory().createExternalResource(persistenceStorageAbstractConfig.getStorageMode());
        if (null == externalResource) return null;
        externalResource.doInit(persistenceStorageAbstractConfig);
        PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = new PersistenceStoreFactory().createStore(
                persistenceStorageAbstractConfig.getConstructType(),
                persistenceStorageAbstractConfig.getStorageMode()
        );
        if (null == store) return null;
        store.doInit(persistenceStorageAbstractConfig, externalResource);
        return store;
    }

    private static String getTtlThreadKey(ConstructType constructType, String constructName) {
        return getConfigKey(constructType, constructName);
    }

    private boolean ttlIsRunning() {
        return !Thread.currentThread().isInterrupted();
    }

    public long findSequence(Ringbuffer<Document> rb, long timestamp) {
        PersistenceStorageAbstractConfig persistenceStorageAbstractConfig = getPersistenceStorageConfig(ConstructType.RINGBUFFER, rb.getName());
        StorageMode storageMode = persistenceStorageAbstractConfig.getStorageMode();
        PersistenceStorageStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>> store = storeImplementationMap.get(getConfigKey(ConstructType.RINGBUFFER, rb.getName()));

        if (store instanceof PersistenceRingBufferStore) {
            return ((PersistenceRingBufferStore<PersistenceStorageAbstractConfig, ExternalResource<PersistenceStorageAbstractConfig>>) store).findSequenceByTimestamp(timestamp);
        }
        if (storageMode == StorageMode.Mem) {
            long headSequence = rb.headSequence();
            long tailSequence = rb.tailSequence();
            if (headSequence == 0L && tailSequence == -1L) {
                return 0L;
            }
            for (long i = headSequence; i <= tailSequence; i++) {
                Document document;
                try {
                    document = rb.readOne(i);
                } catch (InterruptedException e) {
                    break;
                }
                if (null == document) {
                    continue;
                }
                if (document.containsKey("timestamp") && document.get("timestamp") instanceof Long) {
                    Long valueTs = document.getLong("timestamp");
                    if (valueTs >= timestamp) {
                        return i;
                    }
                }
            }
        }
        return rb.tailSequence() + 1L;
    }

    public enum SequenceMode {
        STORE,
        HAZELCAST
    }
}
