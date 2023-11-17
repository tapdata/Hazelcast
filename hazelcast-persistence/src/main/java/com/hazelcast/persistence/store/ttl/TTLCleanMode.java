package com.hazelcast.persistence.store.ttl;

import com.hazelcast.persistence.store.ttl.predicate.IMapKeyFuzzyMatchPredicate;



public enum TTLCleanMode {
    //新增匹配模式，需添加枚举 1.模式名 2.实现的Predicate 3.模式实例类名 4.参数类名,同时需在pdk runner模块添加CleanRuleModel对应的枚举
    FUZZY_MATCHING("fuzzyMatching", IMapKeyFuzzyMatchPredicate.class.getName(),TTLCleanByFuzzyMatch.class.getName(),String.class);

    public String getName() {
        return name;
    }

    private final String name;

    public String getClazz() {
        return clazz;
    }

    public String getTtlCleanRuleClazz() {
        return ttlCleanRuleClazz;
    }

    public Class<?> getCondition() {
        return condition;
    }

    public static TTLCleanMode getTTLCleanMode(String name){
        for(TTLCleanMode ttlCleanMode: TTLCleanMode.values()){
            if(ttlCleanMode.getName().equals(name)){
                return ttlCleanMode;
            }
        }
        return null;
    }

    private final Class<?> condition;

    private final String clazz;

    private final String ttlCleanRuleClazz;

    TTLCleanMode(String name, String clazz, String ttlCleanRuleClazz, Class condition) {
        this.name = name;
        this.clazz = clazz;
        this.ttlCleanRuleClazz = ttlCleanRuleClazz;
        this.condition = condition;
    }
}
