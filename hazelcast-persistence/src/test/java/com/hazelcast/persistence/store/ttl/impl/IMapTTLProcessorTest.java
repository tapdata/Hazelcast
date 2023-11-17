package com.hazelcast.persistence.store.ttl.impl;

import com.hazelcast.persistence.store.PersistenceMapStore;
import com.hazelcast.persistence.store.ttl.*;
import io.jsonwebtoken.lang.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.util.*;

class IMapTTLProcessorTest {
    private IMapTTLProcessor iMapTTLProcessorUnderTest;

    @BeforeEach
    void setUp() {
        iMapTTLProcessorUnderTest = new IMapTTLProcessor();
    }

    /**
     * Normal Process Case
     * @throws InterruptedException
     */
    @Test
    void testCheckTTLCleanRule() throws InterruptedException {
        // Setup
        TTLCleanByFuzzyMatch ttlCleanByFuzzyMatch = new TTLCleanByFuzzyMatch(1L,"check");
        final List<TTLCleanRuleBase> ttlCleanRuleList = Arrays.asList(ttlCleanByFuzzyMatch);
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, ttlCleanByFuzzyMatch);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        // Run the test
        Set<String> resultKey = new HashSet<>();
        //缓存1s
        Thread.sleep(1000);
        iMapTTLProcessorUnderTest.checkTTLCleanRule(ttlCleanRuleList, Arrays.asList(map),
                resultKey, mapStore, ttlProcessorContext, ttlConfig);
        Assert.notEmpty(resultKey);
    }

    @Test
    void testCheckTTLCleanRule_ketEmpty() {
        // Setup
        TTLCleanByFuzzyMatch ttlCleanByFuzzyMatch = new TTLCleanByFuzzyMatch(100L,"check");
        final List<TTLCleanRuleBase> ttlCleanRuleList = Arrays.asList(ttlCleanByFuzzyMatch);
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, ttlCleanByFuzzyMatch);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        // Run the test
        Set<String> resultKey = new HashSet<>();
        iMapTTLProcessorUnderTest.checkTTLCleanRule(ttlCleanRuleList, Arrays.asList(map),
                resultKey, mapStore, ttlProcessorContext, ttlConfig);
        Assert.isTrue(resultKey.isEmpty());
    }
    @Test
    void testCleanExpiredKey_TSisNULL(){
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, null);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        map.put("value",value);
        Set<String> resultKey = new HashSet<>();
        iMapTTLProcessorUnderTest.cleanExpiredKey(map,100L,resultKey,"test",mapStore,ttlProcessorContext,ttlConfig);
        Assert.isTrue(resultKey.isEmpty());
    }

    @Test
    void testCleanExpiredKey_ttlMillisIsZero(){
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, null);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        Set<String> resultKey = new HashSet<>();
        iMapTTLProcessorUnderTest.cleanExpiredKey(map,0L,resultKey,"test",mapStore,ttlProcessorContext,ttlConfig);
        Assert.isTrue(resultKey.isEmpty());
    }

    @Test
    void testCleanExpiredKey_ttlMillisIsNegativeNumber(){
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, null);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        Set<String> resultKey = new HashSet<>();
        iMapTTLProcessorUnderTest.cleanExpiredKey(map,-2,resultKey,"test",mapStore,ttlProcessorContext,ttlConfig);
        Assert.isTrue(resultKey.isEmpty());
    }

    @Test
    void testCleanExpiredKey(){
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, null);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        Set<String> resultKey = new HashSet<>();
        iMapTTLProcessorUnderTest.cleanExpiredKey(map,2000L,resultKey,"test",mapStore,ttlProcessorContext,ttlConfig);
        Assert.isTrue(resultKey.isEmpty());
    }

    @Test
    void testCheckTTLByGlobalExpiration() throws InterruptedException {
        // Setup
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, null);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        // Run the test
        Set<String> resultKey = new HashSet<>();
        //缓存1s
        Thread.sleep(1000);
        // Run the test
        iMapTTLProcessorUnderTest.checkTTLByGlobalExpiration(1000L, Arrays.asList(map),
                resultKey, mapStore, ttlProcessorContext, ttlConfig);

        // Verify the results
        Assert.notEmpty(resultKey);
    }

    @Test
    void testCheckTTLByGlobalExpiration_ketEmpty(){
        // Setup
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, null);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        // Run the test
        Set<String> resultKey = new HashSet<>();
        // Run the test
        iMapTTLProcessorUnderTest.checkTTLByGlobalExpiration(10000L, Arrays.asList(map),
                resultKey, mapStore, ttlProcessorContext, ttlConfig);

        // Verify the results
        Assert.isTrue(resultKey.isEmpty());
    }

    @Test
    void testCheckTTLByGlobalExpiration_ttlIsZero(){
        // Setup
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, null);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        // Run the test
        Set<String> resultKey = new HashSet<>();
        // Run the test
        iMapTTLProcessorUnderTest.checkTTLByGlobalExpiration(0, Arrays.asList(map),
                resultKey, mapStore, ttlProcessorContext, ttlConfig);

        // Verify the results
        Assert.isTrue(resultKey.isEmpty());
    }

    @Test
    void testCheckTTLByGlobalExpiration_ttlMillisIsNegativeNumber(){
        // Setup
        final PersistenceMapStore<?, ?> mapStore = null;
        final TTLProcessorContext ttlProcessorContext = new TTLProcessorContext(null, null);
        final TTLConfig ttlConfig = new TTLConfig(null, 0L, null);
        Map map = new HashMap<String,Object>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        // Run the test
        Set<String> resultKey = new HashSet<>();
        // Run the test
        iMapTTLProcessorUnderTest.checkTTLByGlobalExpiration(-2, Arrays.asList(map),
                resultKey, mapStore, ttlProcessorContext, ttlConfig);

        // Verify the results
        Assert.isTrue(resultKey.isEmpty());
    }


}
