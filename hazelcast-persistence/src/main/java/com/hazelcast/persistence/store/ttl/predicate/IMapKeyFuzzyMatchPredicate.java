package com.hazelcast.persistence.store.ttl.predicate;


import com.hazelcast.persistence.store.ttl.TTLCleanByFuzzyMatch;
import com.hazelcast.persistence.store.ttl.TTLCleanRuleBase;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IMapKeyFuzzyMatchPredicate implements PartitionTTLPredicate {
    @Override
    public boolean isClean(Map<String, Object> map, TTLCleanRuleBase ttlCleanRuleBase) {
        if(ttlCleanRuleBase instanceof TTLCleanByFuzzyMatch && ttlCleanRuleBase.getKeyTTLSeconds() > 0){
            TTLCleanByFuzzyMatch ttlCleanRule = (TTLCleanByFuzzyMatch) ttlCleanRuleBase;
            Pattern pattern = Pattern.compile(ttlCleanRule.getRegex());
            Matcher matcher = pattern.matcher(map.get("key").toString());
            return matcher.find();
        }
        return false;
    }
}
