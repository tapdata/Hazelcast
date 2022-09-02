package com.hazelcast.persistence;

/**
 * @Author dayun
 * @Date 9/2/22
 */
public class MongoDBConfig {
    private String mongoUrl;
    private String collection;
    private String db;

    public String getMongoUrl() {
        return mongoUrl;
    }

    public void setMongoUrl(String mongoUrl) {
        this.mongoUrl = mongoUrl;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
}
