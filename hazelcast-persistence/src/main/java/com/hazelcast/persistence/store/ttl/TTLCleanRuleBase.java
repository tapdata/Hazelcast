package com.hazelcast.persistence.store.ttl;

public abstract class TTLCleanRuleBase {
    private Long keyTTLSeconds;

    private TTLCleanMode type;

    public TTLCleanRuleBase(Long keyTTLSeconds, TTLCleanMode type) {
        this.keyTTLSeconds = keyTTLSeconds;
        this.type = type;
    }

    public Long getKeyTTLSeconds() {
        return keyTTLSeconds;
    }

    public void setKeyTTLSeconds(Long keyTTLSeconds) {
        this.keyTTLSeconds = keyTTLSeconds;
    }

    public TTLCleanMode getType() {
        return type;
    }

    public void setType(TTLCleanMode type) {
        this.type = type;
    }
}
