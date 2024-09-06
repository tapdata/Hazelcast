package com.hazelcast.persistence.store.ttl;

/**
 * @author samuel
 * @Description
 * @create 2024-09-06 10:57
 **/
public class TTLMetrics {
	private String name;
	private Long deleteCount = 0L;
	private Long costMs = 0L;
	private Exception error;

	public TTLMetrics(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public Long getDeleteCount() {
		return deleteCount;
	}

	public Exception getError() {
		return error;
	}

	public Long getCostMs() {
		return costMs;
	}

	public void setDeleteCount(Long deleteCount) {
		this.deleteCount = deleteCount;
	}

	public void setCostMs(Long costMs) {
		this.costMs = costMs;
	}

	public void setError(Exception error) {
		this.error = error;
	}
}
