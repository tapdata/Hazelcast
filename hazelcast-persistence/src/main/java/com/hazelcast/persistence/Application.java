package com.hazelcast.persistence;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.map.IMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.bson.Document;


public class Application {
    public static void main(String[] args) throws InterruptedException {
        Config c = new Config();
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(c);
        Pipeline p = new Pipeline() {
        }
    }
}