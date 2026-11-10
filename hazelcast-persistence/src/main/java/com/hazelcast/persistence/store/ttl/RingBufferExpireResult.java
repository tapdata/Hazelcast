package com.hazelcast.persistence.store.ttl;

/**
 * Result of one {@code deleteExpired} batch on a ring buffer store.
 *
 * <ul>
 *     <li>{@code deletedCount}: number of records deleted in this batch</li>
 *     <li>{@code nextSequence}: start sequence of the next batch</li>
 *     <li>{@code finished}: no need to continue, either an unexpired record was met or the scan reached the upper bound</li>
 * </ul>
 */
public class RingBufferExpireResult {
	private final long deletedCount;
	private final long nextSequence;
	private final boolean finished;

	public RingBufferExpireResult(long deletedCount, long nextSequence, boolean finished) {
		this.deletedCount = deletedCount;
		this.nextSequence = nextSequence;
		this.finished = finished;
	}

	public long getDeletedCount() {
		return deletedCount;
	}

	public long getNextSequence() {
		return nextSequence;
	}

	public boolean isFinished() {
		return finished;
	}

	@Override
	public String toString() {
		return "RingBufferExpireResult{deletedCount=" + deletedCount + ", nextSequence=" + nextSequence + ", finished=" + finished + '}';
	}
}
