package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.PersistenceStorage;
import com.hazelcast.persistence.config.PersistenceDuckDBConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.impl.DuckDBResource;
import com.hazelcast.persistence.store.PersistenceRingBufferStore;
import org.bson.Document;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class DuckDBRingBuffer extends PersistenceRingBufferStore<PersistenceDuckDBConfig, DuckDBResource> {
	private static final String TABLE_NAME = "hz_ringbuffer_store";
	private static final String VALUE_KEY = "value";
	private static final int STORE_ALL_BATCH_SIZE = 1000;

	private final AtomicLong largestSequence = new AtomicLong(-1L);
	private final AtomicLong smallestSequence = new AtomicLong(0L);

	private DuckDBResource duckDBResource;
	private PersistenceDuckDBConfig persistenceDuckDBConfig;

	@Override
	public void doInit(PersistenceDuckDBConfig persistenceDuckDBConfig, DuckDBResource duckDBResource) {
		super.doInit(persistenceDuckDBConfig, duckDBResource);
		this.duckDBResource = duckDBResource;
		this.persistenceDuckDBConfig = persistenceDuckDBConfig;
		createSchemaIfNeeded();
		flushSequence();
	}

	@Override
	public void lightInit() {
		super.lightInit();
		createSchemaIfNeeded();
		flushSequence();
	}

	@Override
	public void reInitResource(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
		if (persistenceStorageAbstractConfig instanceof PersistenceDuckDBConfig newConfig) {
			duckDBResource.reInit(newConfig);
			this.persistenceDuckDBConfig = newConfig;
			createSchemaIfNeeded();
			flushSequence();
		}
	}

	private void createSchemaIfNeeded() {
		if (duckDBResource == null) {
			return;
		}
		try (Statement statement = duckDBResource.getConnection().createStatement()) {
			statement.execute(
					"CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
							"ringbuffer VARCHAR NOT NULL, " +
							"seq BIGINT NOT NULL, " +
							"v VARCHAR, " +
							"ts BIGINT, " +
							"event_ts BIGINT, " +
							"type VARCHAR, " +
							"PRIMARY KEY (ringbuffer, seq)" +
							")"
			);
			statement.execute("CREATE INDEX IF NOT EXISTS idx_" + TABLE_NAME + "_rb_seq ON " + TABLE_NAME + " (ringbuffer, seq)");
			statement.execute("CREATE INDEX IF NOT EXISTS idx_" + TABLE_NAME + "_rb_eventts ON " + TABLE_NAME + " (ringbuffer, event_ts)");
			statement.execute("CREATE INDEX IF NOT EXISTS idx_" + TABLE_NAME + "_rb_ts ON " + TABLE_NAME + " (ringbuffer, ts)");
		} catch (SQLException e) {
			throw new RuntimeException("Init DuckDBRingBuffer schema failed, config: " + persistenceDuckDBConfig, e);
		}
	}

	private void flushSequence() {
		this.smallestSequence.set(_getSmallestSequence());
		this.largestSequence.set(_getLargestSequence());
	}

	@Override
	public void doDestroy() {
		Optional.ofNullable(this.duckDBResource).ifPresent(resource -> CommonUtils.handleWithError(
				() -> {
					resource.close();
					this.duckDBResource = null;
				},
				throwable -> {
					throw new RuntimeException("Close Ringbuffer[" + ringBufferName + "]'s DuckDB resource failed, config: " + persistenceDuckDBConfig, throwable);
				}
		));
	}

	@Override
	public void store(long sequence, Object value) {
		if (!checkEnable() || duckDBResource == null) {
			return;
		}
		if (!(value instanceof Document document)) {
			return;
		}
		long seqToStore = sequence;
		if (persistenceDuckDBConfig.getSequenceMode() == PersistenceStorage.SequenceMode.STORE) {
			seqToStore = largestSequence.incrementAndGet();
		} else {
			largestSequence.accumulateAndGet(seqToStore, Math::max);
		}
		insertOne(seqToStore, document);
	}

	@Override
	public void storeAll(long sequence, Object[] values) {
		if (!checkEnable() || duckDBResource == null) {
			return;
		}
		if (values == null || values.length == 0) {
			return;
		}
		String sql = "INSERT INTO " + TABLE_NAME + " (ringbuffer, seq, v, ts, event_ts, type) VALUES (?, ?, ?, ?, ?, ?) " +
				"ON CONFLICT (ringbuffer, seq) DO UPDATE SET v = excluded.v, ts = excluded.ts, event_ts = excluded.event_ts, type = excluded.type";
		try (PreparedStatement ps = duckDBResource.getConnection().prepareStatement(sql)) {
			int i = 0;
			for (Object value : values) {
				if (!(value instanceof Document document)) {
					continue;
				}
				long seqToStore;
				if (persistenceDuckDBConfig.getSequenceMode() == PersistenceStorage.SequenceMode.STORE) {
					seqToStore = largestSequence.incrementAndGet();
				} else {
					seqToStore = sequence + i;
					largestSequence.accumulateAndGet(seqToStore, Math::max);
				}
				long ts = System.currentTimeMillis() / 1000;
				document.append("_ts", ts);
				Long eventTs = getLong(document.get("timestamp"));
				String type = getString(document.get("type"));

				ps.setString(1, ringBufferName);
				ps.setLong(2, seqToStore);
				ps.setString(3, document.toJson());
				ps.setLong(4, ts);
				if (eventTs == null) {
					ps.setNull(5, java.sql.Types.BIGINT);
				} else {
					ps.setLong(5, eventTs);
				}
				ps.setString(6, type);
				ps.addBatch();

				i++;
				if (i % STORE_ALL_BATCH_SIZE == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException("StoreAll to DuckDB ringbuffer failed, ringbuffer: " + ringBufferName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	private void insertOne(long sequence, Document document) {
		long ts = System.currentTimeMillis() / 1000;
		document.append("_ts", ts);
		Long eventTs = getLong(document.get("timestamp"));
		String type = getString(document.get("type"));

		String sql = "INSERT INTO " + TABLE_NAME + " (ringbuffer, seq, v, ts, event_ts, type) VALUES (?, ?, ?, ?, ?, ?) " +
				"ON CONFLICT (ringbuffer, seq) DO UPDATE SET v = excluded.v, ts = excluded.ts, event_ts = excluded.event_ts, type = excluded.type";
		try (PreparedStatement ps = duckDBResource.getConnection().prepareStatement(sql)) {
			ps.setString(1, ringBufferName);
			ps.setLong(2, sequence);
			ps.setString(3, document.toJson());
			ps.setLong(4, ts);
			if (eventTs == null) {
				ps.setNull(5, java.sql.Types.BIGINT);
			} else {
				ps.setLong(5, eventTs);
			}
			ps.setString(6, type);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Store to DuckDB ringbuffer failed, ringbuffer: " + ringBufferName + ", seq: " + sequence + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public Document load(long sequence) {
		if (!checkEnable() || duckDBResource == null) {
			return null;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT v FROM " + TABLE_NAME + " WHERE ringbuffer = ? AND seq = ?")) {
			ps.setString(1, ringBufferName);
			ps.setLong(2, sequence);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return null;
				}
				String json = rs.getString(1);
				if (json == null) {
					return null;
				}
				return Document.parse(json);
			}
		} catch (SQLException e) {
			throw new RuntimeException("Load from DuckDB ringbuffer failed, ringbuffer: " + ringBufferName + ", seq: " + sequence + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public void delete(long sequence) {
		if (!checkEnable() || duckDBResource == null) {
			return;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("DELETE FROM " + TABLE_NAME + " WHERE ringbuffer = ? AND seq = ?")) {
			ps.setString(1, ringBufferName);
			ps.setLong(2, sequence);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Delete from DuckDB ringbuffer failed, ringbuffer: " + ringBufferName + ", seq: " + sequence + ", config: " + persistenceDuckDBConfig, e);
		} finally {
			flushSequence();
		}
	}

	@Override
	public long getLargestSequence() {
		return largestSequence.get();
	}

	private long _getLargestSequence() {
		if (duckDBResource == null) {
			return -1L;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT MAX(seq) FROM " + TABLE_NAME + " WHERE ringbuffer = ?")) {
			ps.setString(1, ringBufferName);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return -1L;
				}
				long max = rs.getLong(1);
				if (rs.wasNull()) {
					return -1L;
				}
				return max;
			}
		} catch (SQLException e) {
			throw new RuntimeException("Get largest sequence from DuckDB ringbuffer failed, ringbuffer: " + ringBufferName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public long getSmallestSequence() {
		return smallestSequence.get();
	}

	private long _getSmallestSequence() {
		if (duckDBResource == null) {
			return 0L;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT MIN(seq) FROM " + TABLE_NAME + " WHERE ringbuffer = ?")) {
			ps.setString(1, ringBufferName);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return 0L;
				}
				long min = rs.getLong(1);
				if (rs.wasNull()) {
					return 0L;
				}
				return min;
			}
		} catch (SQLException e) {
			throw new RuntimeException("Get smallest sequence from DuckDB ringbuffer failed, ringbuffer: " + ringBufferName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public long getSmallestSequenceWithoutSign() {
		if (!checkEnable() || duckDBResource == null) {
			return 0L;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT MIN(seq) FROM " + TABLE_NAME + " WHERE ringbuffer = ? AND (type IS NULL OR type <> 'SIGN')")) {
			ps.setString(1, ringBufferName);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return 0L;
				}
				long min = rs.getLong(1);
				if (rs.wasNull()) {
					return 0L;
				}
				return min;
			}
		} catch (SQLException e) {
			throw new RuntimeException("Get smallest sequence without SIGN from DuckDB ringbuffer failed, ringbuffer: " + ringBufferName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public long findSequenceByTimestamp(long timestamp) {
		if (!checkEnable() || duckDBResource == null) {
			return 0L;
		}
		flushSequence();
		if (largestSequence.get() < 0) {
			return 0L;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT seq FROM " + TABLE_NAME + " WHERE ringbuffer = ? AND event_ts >= ? ORDER BY seq ASC LIMIT 1")) {
			ps.setString(1, ringBufferName);
			ps.setLong(2, timestamp);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return largestSequence.get() + 1L;
				}
				return rs.getLong(1);
			}
		} catch (SQLException e) {
			throw new RuntimeException("Find sequence by timestamp from DuckDB ringbuffer failed, ringbuffer: " + ringBufferName + ", timestamp: " + timestamp + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public boolean isEmpty() {
		if (!checkEnable() || duckDBResource == null) {
			return true;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT 1 FROM " + TABLE_NAME + " WHERE ringbuffer = ? LIMIT 1")) {
			ps.setString(1, ringBufferName);
			try (ResultSet rs = ps.executeQuery()) {
				return !rs.next();
			}
		} catch (SQLException e) {
			throw new RuntimeException("Check DuckDB ringbuffer isEmpty failed, ringbuffer: " + ringBufferName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public Map<String, Object> getStatistics() {
		if (!checkEnable() || duckDBResource == null) {
			return null;
		}
		Map<String, Object> statistics = new HashMap<>();
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT COUNT(*), MIN(seq), MAX(seq) FROM " + TABLE_NAME + " WHERE ringbuffer = ?")) {
			ps.setString(1, ringBufferName);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					statistics.put("count", rs.getLong(1));
					long min = rs.getLong(2);
					statistics.put("smallestSequence", rs.wasNull() ? 0L : min);
					long max = rs.getLong(3);
					statistics.put("largestSequence", rs.wasNull() ? -1L : max);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Get DuckDB ringbuffer statistics failed, ringbuffer: " + ringBufferName + ", config: " + persistenceDuckDBConfig, e);
		}
		statistics.put("uri", persistenceDuckDBConfig.uriInfo());
		statistics.put("mode", persistenceDuckDBConfig.getStorageMode().name());
		statistics.put("inMemory", persistenceDuckDBConfig.isInMemory());
		return statistics;
	}

	private static Long getLong(Object obj) {
		if (obj == null) {
			return null;
		}
		if (obj instanceof Long l) {
			return l;
		}
		if (obj instanceof Integer i) {
			return i.longValue();
		}
		if (obj instanceof Number n) {
			return n.longValue();
		}
		try {
			return Long.parseLong(obj.toString());
		} catch (Exception e) {
			return null;
		}
	}

	private static String getString(Object obj) {
		if (obj == null) {
			return null;
		}
		return obj.toString();
	}
}
