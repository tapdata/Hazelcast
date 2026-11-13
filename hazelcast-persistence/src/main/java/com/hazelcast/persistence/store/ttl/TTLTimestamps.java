package com.hazelcast.persistence.store.ttl;

import java.util.Map;

/**
 * Single place that decodes the TTL timestamp {@code _ts} (seconds) for both IMap and ring buffer TTL.
 * <p>
 * Lookup: the document itself, or its {@code value} sub document when present.
 * Only integral BSON numbers are accepted (Int32 / Int64), same as the original IMap rule;
 * any other type (string, double, ...) is treated as "no timestamp" and never expires.
 */
public final class TTLTimestamps {
	public static final String TS_FIELD = "_ts";
	private static final String VALUE_FIELD = "value";

	private TTLTimestamps() {
	}

	/**
	 * @return {@code _ts} as long, or null when absent or not an Int32/Int64
	 */
	public static Long read(Map<?, ?> document) {
		if (null == document) {
			return null;
		}
		Map<?, ?> value = document.get(VALUE_FIELD) instanceof Map<?, ?> map ? map : document;
		Object ts = value.get(TS_FIELD);
		if (ts instanceof Long l) {
			return l;
		}
		if (ts instanceof Integer i) {
			return i.longValue();
		}
		return null;
	}
}
