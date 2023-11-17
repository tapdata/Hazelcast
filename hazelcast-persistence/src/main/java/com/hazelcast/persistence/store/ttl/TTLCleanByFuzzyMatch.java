package com.hazelcast.persistence.store.ttl;


public class TTLCleanByFuzzyMatch extends TTLCleanRuleBase {
    private String regex;

    public TTLCleanByFuzzyMatch(Long keyTTLSeconds, String regex) {
        super(keyTTLSeconds,TTLCleanMode.FUZZY_MATCHING);
        this.regex = regex;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }
}
