package com.hazelcast.persistence.store.impl;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MongoDBRingBufferExpireTest {
	private static final long CUTOFF = 1_000L;
	private static final long EXPIRED = 500L;
	private static final long FRESH = 2_000L;

	private static Document doc(Object key, Document value) {
		Document doc = new Document("key", key);
		if (null != value) {
			doc.append("value", value);
		}
		return doc;
	}

	private static Document data(Object ts) {
		Document value = new Document("type", "DATA");
		if (null != ts) {
			value.append("_ts", ts);
		}
		return value;
	}

	private static Document sign() {
		return new Document("type", "SIGN").append("_ts", EXPIRED);
	}

	@Test
	void allExpired_fullBatch_continuesAfterLastKey() {
		List<Document> docs = List.of(doc(10L, data(EXPIRED)), doc(11L, data(EXPIRED)), doc(12L, data(EXPIRED)));
		MongoDBRingBuffer.ExpireScan scan = MongoDBRingBuffer.scanExpireBatch(docs, 10L, CUTOFF, 3);
		assertFalse(scan.finished);
		assertEquals(12L, scan.lastExpiredKey);
		assertEquals(13L, scan.nextSequence);
	}

	@Test
	void partialBatch_finished() {
		List<Document> docs = List.of(doc(10L, data(EXPIRED)), doc(11L, data(EXPIRED)));
		MongoDBRingBuffer.ExpireScan scan = MongoDBRingBuffer.scanExpireBatch(docs, 10L, CUTOFF, 5);
		assertTrue(scan.finished);
		assertEquals(11L, scan.lastExpiredKey);
	}

	@Test
	void freshInTheMiddleOfFullBatch_nextSequenceIsFreshKeyNotBatchTail() {
		List<Document> docs = List.of(doc(1L, data(EXPIRED)), doc(2L, data(FRESH)), doc(3L, data(EXPIRED)));
		MongoDBRingBuffer.ExpireScan scan = MongoDBRingBuffer.scanExpireBatch(docs, 1L, CUTOFF, 3);
		assertTrue(scan.finished);
		assertEquals(1L, scan.lastExpiredKey);
		assertEquals(2L, scan.nextSequence);
	}

	@Test
	void firstIsFresh_nothingToDelete() {
		MongoDBRingBuffer.ExpireScan scan = MongoDBRingBuffer.scanExpireBatch(List.of(doc(5L, data(FRESH))), 5L, CUTOFF, 10);
		assertTrue(scan.finished);
		assertNull(scan.lastExpiredKey);
		assertEquals(5L, scan.nextSequence);
	}

	@Test
	void signAndNoTs_skippedButFollowingExpiredIncluded() {
		List<Document> docs = List.of(doc(0L, sign()), doc(1L, data(null)), doc(2L, data("x")), doc(3L, null), doc(4L, data(EXPIRED)));
		MongoDBRingBuffer.ExpireScan scan = MongoDBRingBuffer.scanExpireBatch(docs, 0L, CUTOFF, 5);
		assertEquals(4L, scan.lastExpiredKey);
		assertFalse(scan.finished);
		assertEquals(5L, scan.nextSequence);
	}

	@Test
	void onlySignInFullBatch_continues() {
		List<Document> docs = List.of(doc(0L, sign()), doc(1L, sign()));
		MongoDBRingBuffer.ExpireScan scan = MongoDBRingBuffer.scanExpireBatch(docs, 0L, CUTOFF, 2);
		assertNull(scan.lastExpiredKey);
		assertFalse(scan.finished);
		assertEquals(2L, scan.nextSequence);
	}

	@Test
	void holesInKeys_followDocumentOrder() {
		List<Document> docs = List.of(doc(100L, data(EXPIRED)), doc(5_000L, data(EXPIRED)), doc(90_000L, data(EXPIRED)));
		MongoDBRingBuffer.ExpireScan scan = MongoDBRingBuffer.scanExpireBatch(docs, 0L, CUTOFF, 3);
		assertEquals(90_000L, scan.lastExpiredKey);
		assertEquals(90_001L, scan.nextSequence);
		assertFalse(scan.finished);
	}

	@Test
	void int32KeyAndTs_compatible() {
		List<Document> docs = List.of(doc(1, data((int) EXPIRED)), doc(2, data((int) FRESH)));
		MongoDBRingBuffer.ExpireScan scan = assertDoesNotThrow(() -> MongoDBRingBuffer.scanExpireBatch(docs, 1L, CUTOFF, 10));
		assertEquals(1L, scan.lastExpiredKey);
		assertEquals(2L, scan.nextSequence);
		assertTrue(scan.finished);
	}

	@Test
	void tsEqualsCutoff_isExpired() {
		MongoDBRingBuffer.ExpireScan scan = MongoDBRingBuffer.scanExpireBatch(List.of(doc(1L, data(CUTOFF))), 1L, CUTOFF, 10);
		assertEquals(1L, scan.lastExpiredKey);
	}

	@Test
	void emptyOrInvalidKeys_finishedWithoutProgress() {
		MongoDBRingBuffer.ExpireScan empty = MongoDBRingBuffer.scanExpireBatch(new ArrayList<>(), 7L, CUTOFF, 10);
		assertTrue(empty.finished);
		assertEquals(7L, empty.nextSequence);
		MongoDBRingBuffer.ExpireScan invalid = MongoDBRingBuffer.scanExpireBatch(List.of(doc("abc", data(EXPIRED))), 7L, CUTOFF, 1);
		assertTrue(invalid.finished);
		assertNull(invalid.lastExpiredKey);
	}
}
