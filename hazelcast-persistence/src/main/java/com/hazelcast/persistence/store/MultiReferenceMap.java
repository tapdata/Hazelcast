package com.hazelcast.persistence.store;

import org.apache.commons.collections4.CollectionUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/5/19 17:42 Create
 */
public abstract class MultiReferenceMap<K, V> {
    public static final String DEFAULT_REFERENCE_ID = "default";
    private final Map<K, Set<String>> referenceMap = new ConcurrentHashMap<>();
    private final Map<K, V> dataMap = new ConcurrentHashMap<>();
    private final Map<K, ReentrantLock> lockMap = new ConcurrentHashMap<>();

    public V init(String referenceId, K key, V value) {
        synchronized (this) {
            referenceMap.computeIfAbsent(key, k -> {
                Set<String> referenceSet = new HashSet<>();
                referenceSet.add(referenceId);
                dataMap.put(key, value);
                return referenceSet;
            });
            return dataMap.get(key);
        }
    }

    public V addReference(String referenceId, K key) {
        synchronized (this) {
            V val = dataMap.get(key);
            if (null == val) {
                throw new RuntimeException(String.format("Reference '%s' key '%s' is not exists", referenceId, key));
            }
            referenceMap.computeIfPresent(key, (k, v) -> {
                v.add(referenceId);
                return v;
            });
            return val;
        }

    }

    public V get(K key) {
        return dataMap.get(key);
    }

    public boolean containsKey(K key) {
        return dataMap.containsKey(key);
    }

    public boolean destroy(String referenceId, K key) {
        synchronized (this) {
            AtomicBoolean result = new AtomicBoolean(false);
            referenceMap.computeIfPresent(key, (k, v) -> {
                v.remove(referenceId);
                if (CollectionUtils.isEmpty(v)) {
                    destroyValue(dataMap.get(key));
                    result.set(true);
                }
                return v;
            });
            return result.get();
        }
    }

    public void lock(K key) throws InterruptedException {
        lockMap.computeIfAbsent(key, k -> new ReentrantLock()).lockInterruptibly();
    }

    public void unlock(K key) {
        lockMap.computeIfPresent(key, (k, v) -> {
			v.unlock();
			return v;
        });
    }

    public void removeLock(K key) {
        // check referenceMap
        synchronized (this) {
            if(null != referenceMap.get(key) && !referenceMap.get(key).isEmpty()) {
                return;
            }
            if(null != lockMap.get(key) && !lockMap.get(key).isLocked()) {
                lockMap.remove(key);
            }
        }
    }

    protected abstract void destroyValue(V value);
}
