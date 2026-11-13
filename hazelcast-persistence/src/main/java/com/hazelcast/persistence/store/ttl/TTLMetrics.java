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
	private Throwable error;
	/**
	 * True when this round stopped before draining all expired data (duration limit or interrupted),
	 * the remaining data is left to the next round.
	 */
	private boolean hasMore;
	/**
	 * Batches that failed and were retried with a smaller batch size in this round
	 */
	private int batchRetries;
	/**
	 * Effective batch size at the end of this round
	 */
	private int batchSize;
	private String lastBatchError;

	public TTLMetrics(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public Long getDeleteCount() {
		return deleteCount;
	}

	public Throwable getError() {
		return error;
	}

	public Long getCostMs() {
		return costMs;
	}

	public boolean isHasMore() {
		return hasMore;
	}

	public void setDeleteCount(Long deleteCount) {
		this.deleteCount = deleteCount;
	}

	public void setCostMs(Long costMs) {
		this.costMs = costMs;
	}

	public void setError(Throwable error) {
		this.error = error;
	}

	public void setHasMore(boolean hasMore) {
		this.hasMore = hasMore;
	}

	public int getBatchRetries() {
		return batchRetries;
	}

	public void setBatchRetries(int batchRetries) {
		this.batchRetries = batchRetries;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public String getLastBatchError() {
		return lastBatchError;
	}

	public void setLastBatchError(String lastBatchError) {
		this.lastBatchError = lastBatchError;
	}
}
