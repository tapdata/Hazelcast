package com.hazelcast.persistence.store.ttl.predicate;

import com.hazelcast.persistence.store.ttl.TTLCleanRuleBase;

import java.util.Map;

public interface PartitionTTLPredicate {
    boolean isClean(Map<String, Object> map, TTLCleanRuleBase ttlCleanRuleBase);
}
