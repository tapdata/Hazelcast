package com.hazelcast.persistence.store.ttl;

import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2023-09-21 17:02
 **/
public class TTLConfig {
	private PersistenceStorageAbstractConfig persistenceStorageAbstractConfig;
	private long ttlSeconds;

	private List<TTLCleanRuleBase> ttlCleanRuleList = new ArrayList<>();
	public TTLConfig(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig, long ttlSeconds){
		this.persistenceStorageAbstractConfig = persistenceStorageAbstractConfig;
		this.ttlSeconds = ttlSeconds;
	}
	public TTLConfig(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig,long ttlSeconds,TTLCleanRuleBase ttlCleanRuleBase) {
		this.persistenceStorageAbstractConfig = persistenceStorageAbstractConfig;
		this.ttlSeconds = ttlSeconds;
		ttlCleanRuleList.add(ttlCleanRuleBase);
	}

	public PersistenceStorageAbstractConfig getPersistenceStorageAbstractConfig() {
		return persistenceStorageAbstractConfig;
	}

	public long getTtlSeconds() {
		return ttlSeconds;
	}

	public void addTTLCleanRule(TTLCleanRuleBase ttlCleanRuleBase){
		ttlCleanRuleList.add(ttlCleanRuleBase);
	}

	public List<TTLCleanRuleBase> getTtlCleanRuleList(){
		return ttlCleanRuleList;
	}

	public void setTtlSeconds(long ttlSeconds) {
		this.ttlSeconds = ttlSeconds;
	}


}
