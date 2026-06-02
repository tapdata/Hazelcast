package com.hazelcast.persistence.store;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Query parameter for {@link PersistenceStorageStore#find(RingBufferFindParam, int)}.
 * Carries only storage-level fields; type conversion is handled by the caller.
 */
@Data
public class RingBufferFindParam {
	private String ringBuffer;
	private String connectionId;
	private String tableName;
	private Long key;
	private long startTime;
	private long endTime;
	private List<Map<String, Object>> filters;
}
