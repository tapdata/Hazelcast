package com.hazelcast.persistence.store;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/5/19 17:42 Create
 */
public abstract class MultiReferenceMap<K, V> {
    public static final String DEFAULT_REFERENCE_ID = "default";
    private final Set<String> referenceSet = new HashSet<>();
    private final Map<K, V> dataMap = new ConcurrentHashMap<>();

    public V init(String referenceId, K key, V value) {
        synchronized (referenceSet) {
            if (dataMap.containsKey(key)) {
                throw new RuntimeException(String.format("Reference '%s' key '%s' is exists", referenceId, key));
            }
            referenceSet.add(referenceId);
            return dataMap.put(key, value);
        }
    }

    public V addReference(String referenceId, K key) {
        synchronized (referenceSet) {
            V val = dataMap.get(key);
            if (null == val) {
                throw new RuntimeException(String.format("Reference '%s' key '%s' is not exists", referenceId, key));
            }
            referenceSet.add(referenceId);
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
        synchronized (referenceSet) {
            referenceSet.remove(referenceId);
            if (referenceSet.isEmpty()) {
                Optional.ofNullable(dataMap.remove(key)).ifPresent(this::destroyValue);
								return true;
            }
        }
				return false;
    }

    protected abstract void destroyValue(V value);
}
