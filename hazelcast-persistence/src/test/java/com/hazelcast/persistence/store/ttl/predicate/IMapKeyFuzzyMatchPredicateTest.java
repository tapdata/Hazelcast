package com.hazelcast.persistence.store.ttl.predicate;

import com.hazelcast.persistence.store.ttl.TTLCleanByFuzzyMatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IMapKeyFuzzyMatchPredicateTest {

    private IMapKeyFuzzyMatchPredicate iMapKeyFuzzyMatchPredicateUnderTest;

    @BeforeEach
    void setUp() {
        iMapKeyFuzzyMatchPredicateUnderTest = new IMapKeyFuzzyMatchPredicate();
    }

    @Test
    void testIsClean_Mismatch() {
        // Setup
        final Map<String, Object> map = new HashMap<>();
        map.put("key","check");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        final TTLCleanByFuzzyMatch ttlCleanByFuzzyMatch = new TTLCleanByFuzzyMatch(1L,"text") ;

        // Run the test
        final boolean result = iMapKeyFuzzyMatchPredicateUnderTest.isClean(map, ttlCleanByFuzzyMatch);

        // Verify the results
        assertFalse(result);
    }
    @Test
    void testIsClean_match() {
        // Setup
        final Map<String, Object> map = new HashMap<>();
        map.put("key","text");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        final TTLCleanByFuzzyMatch ttlCleanByFuzzyMatch = new TTLCleanByFuzzyMatch(1L,"text") ;

        // Run the test
        final boolean result = iMapKeyFuzzyMatchPredicateUnderTest.isClean(map, ttlCleanByFuzzyMatch);

        // Verify the results
        assertTrue(result);
    }

    @Test
    void testIsClean_ketTTLIsZero() {
        // Setup
        final Map<String, Object> map = new HashMap<>();
        map.put("key","text");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        final TTLCleanByFuzzyMatch ttlCleanByFuzzyMatch = new TTLCleanByFuzzyMatch(0L,"text") ;

        // Run the test
        final boolean result = iMapKeyFuzzyMatchPredicateUnderTest.isClean(map, ttlCleanByFuzzyMatch);

        // Verify the results
        assertFalse(result);
    }

    @Test
    void testIsClean_ketTTLIsNegativeNumber() {
        // Setup
        final Map<String, Object> map = new HashMap<>();
        map.put("key","text");
        Map value = new HashMap<>();
        value.put("_ts",System.currentTimeMillis()/1000);
        map.put("value",value);
        final TTLCleanByFuzzyMatch ttlCleanByFuzzyMatch = new TTLCleanByFuzzyMatch(0L,"text") ;

        // Run the test
        final boolean result = iMapKeyFuzzyMatchPredicateUnderTest.isClean(map, ttlCleanByFuzzyMatch);

        // Verify the results
        assertFalse(result);
    }
}
