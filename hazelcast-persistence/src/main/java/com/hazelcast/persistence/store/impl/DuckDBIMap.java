package com.hazelcast.persistence.store.impl;

import com.hazelcast.persistence.CommonUtils;
import com.hazelcast.persistence.config.PersistenceDuckDBConfig;
import com.hazelcast.persistence.config.PersistenceStorageAbstractConfig;
import com.hazelcast.persistence.resource.impl.DuckDBResource;
import com.hazelcast.persistence.store.PersistenceMapStore;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;

public class DuckDBIMap extends PersistenceMapStore<PersistenceDuckDBConfig, DuckDBResource> {
	private static final int STORE_ALL_BATCH_SIZE = 1000;
	private static final int DELETE_ALL_BATCH_SIZE = 100;
	private static final int QUERY_BATCH_SIZE = 20;
	private static final String TABLE_NAME = "hz_imap_store";
	public static final String VALUE_KEY = "value";

	private DuckDBResource duckDBResource;
	private PersistenceDuckDBConfig persistenceDuckDBConfig;

	public DuckDBIMap() {
	}

	@Override
	public void doInit(PersistenceDuckDBConfig persistenceDuckDBConfig, DuckDBResource duckDBResource) {
		super.doInit(persistenceDuckDBConfig, duckDBResource);
		this.duckDBResource = duckDBResource;
		this.persistenceDuckDBConfig = persistenceDuckDBConfig;
		createSchemaIfNeeded();
	}

	@Override
	public void lightInit() {
		super.lightInit();
		createSchemaIfNeeded();
	}

	@Override
	public void reInitResource(PersistenceStorageAbstractConfig persistenceStorageAbstractConfig) {
		if (persistenceStorageAbstractConfig instanceof PersistenceDuckDBConfig newConfig) {
			duckDBResource.reInit(newConfig);
			this.persistenceDuckDBConfig = newConfig;
		}
	}

	private void createSchemaIfNeeded() {
		if (duckDBResource == null) {
			return;
		}
		try (Statement statement = duckDBResource.getConnection().createStatement()) {
			statement.execute(
					"CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
							"imap VARCHAR NOT NULL, " +
							"k VARCHAR NOT NULL, " +
							"v VARCHAR, " +
							"ts BIGINT, " +
							"PRIMARY KEY (imap, k)" +
							")"
			);
			statement.execute("CREATE INDEX IF NOT EXISTS idx_" + TABLE_NAME + "_imap_ts ON " + TABLE_NAME + " (imap, ts)");
		} catch (SQLException e) {
			throw new RuntimeException("Init DuckDBIMap schema failed, config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public void doClear() {
		deleteAll(null);
	}

	@Override
	public void doDestroy() {
		releaseResource();
	}

	@Override
	public void destroy() {
		deleteAll(null);
		releaseResource();
	}

	private synchronized void releaseResource() {
		Optional.ofNullable(this.duckDBResource).ifPresent(resource -> CommonUtils.handleWithError(
				() -> {
					resource.close();
					this.duckDBResource = null;
				},
				throwable -> {
					throw new RuntimeException("Close IMap[" + imapName + "]'s DuckDB resource failed, config: " + persistenceDuckDBConfig, throwable);
				}
		));
	}

	public synchronized void store(String key, Object value) {
		if (!checkEnable() || duckDBResource == null) {
			return;
		}
		if (!(value instanceof Document document)) {
			return;
		}
		long ts = System.currentTimeMillis() / 1000;
		document.append("_ts", ts);

		String sql = "INSERT INTO " + TABLE_NAME + " (imap, k, v, ts) VALUES (?, ?, ?, ?) " +
				"ON CONFLICT (imap, k) DO UPDATE SET v = excluded.v, ts = excluded.ts";
		try (PreparedStatement ps = duckDBResource.getConnection().prepareStatement(sql)) {
			ps.setString(1, imapName);
			ps.setString(2, key);
			ps.setString(3, document.toJson());
			ps.setLong(4, ts);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Store to DuckDB failed, imap: " + imapName + ", key: " + key + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	public synchronized void storeAll(Map<String, Object> map) {
		if (!checkEnable() || duckDBResource == null) {
			return;
		}
		if (map == null || map.isEmpty()) {
			return;
		}
		long ts = System.currentTimeMillis() / 1000;
		String sql = "INSERT INTO " + TABLE_NAME + " (imap, k, v, ts) VALUES (?, ?, ?, ?) " +
				"ON CONFLICT (imap, k) DO UPDATE SET v = excluded.v, ts = excluded.ts";
		try (PreparedStatement ps = duckDBResource.getConnection().prepareStatement(sql)) {
			int i = 0;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				Object value = entry.getValue();
				if (!(value instanceof Document document)) {
					continue;
				}
				document.append("_ts", ts);
				ps.setString(1, imapName);
				ps.setString(2, entry.getKey());
				ps.setString(3, document.toJson());
				ps.setLong(4, ts);
				ps.addBatch();
				i++;
				if (i % STORE_ALL_BATCH_SIZE == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException("StoreAll to DuckDB failed, imap: " + imapName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	public synchronized void delete(String key) {
		if (!checkEnable() || duckDBResource == null) {
			return;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("DELETE FROM " + TABLE_NAME + " WHERE imap = ? AND k = ?")) {
			ps.setString(1, imapName);
			ps.setString(2, key);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Delete from DuckDB failed, imap: " + imapName + ", key: " + key + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	public synchronized void deleteAll(Collection<String> keys) {
		if (!checkEnable() || duckDBResource == null) {
			return;
		}
		if (CollectionUtils.isEmpty(keys)) {
			try (PreparedStatement ps = duckDBResource.getConnection()
					.prepareStatement("DELETE FROM " + TABLE_NAME + " WHERE imap = ?")) {
				ps.setString(1, imapName);
				ps.executeUpdate();
			} catch (SQLException e) {
				throw new RuntimeException("DeleteAll from DuckDB failed, imap: " + imapName + ", config: " + persistenceDuckDBConfig, e);
			}
			return;
		}

		List<String> batch = new ArrayList<>(DELETE_ALL_BATCH_SIZE);
		for (String key : keys) {
			if (key == null) {
				continue;
			}
			batch.add(key);
			if (batch.size() >= DELETE_ALL_BATCH_SIZE) {
				deleteKeyBatch(batch);
				batch.clear();
			}
		}
		if (!batch.isEmpty()) {
			deleteKeyBatch(batch);
			batch.clear();
		}
	}

	private void deleteKeyBatch(List<String> keys) {
		String placeholders = String.join(",", Collections.nCopies(keys.size(), "?"));
		String sql = "DELETE FROM " + TABLE_NAME + " WHERE imap = ? AND k IN (" + placeholders + ")";
		try (PreparedStatement ps = duckDBResource.getConnection().prepareStatement(sql)) {
			ps.setString(1, imapName);
			for (int i = 0; i < keys.size(); i++) {
				ps.setString(i + 2, keys.get(i));
			}
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("DeleteAll from DuckDB failed, imap: " + imapName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	public synchronized Document load(String key) {
		if (!checkEnable() || duckDBResource == null) {
			return null;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT v FROM " + TABLE_NAME + " WHERE imap = ? AND k = ?")) {
			ps.setString(1, imapName);
			ps.setString(2, key);
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
			throw new RuntimeException("Load from DuckDB failed, imap: " + imapName + ", key: " + key + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	public synchronized Map<String, Object> loadAll(Collection<String> keys) {
		if (!checkEnable() || duckDBResource == null || CollectionUtils.isEmpty(keys)) {
			return null;
		}
		Map<String, Object> result = new HashMap<>();
		Collection<String> cache = new HashSet<>();
		for (String key : keys) {
			if (key == null) {
				continue;
			}
			cache.add(key);
			if (cache.size() >= QUERY_BATCH_SIZE) {
				loadAll(cache, result);
				cache.clear();
			}
		}
		if (!cache.isEmpty()) {
			loadAll(cache, result);
			cache.clear();
		}
		return result;
	}

	private void loadAll(Collection<String> keys, Map<String, Object> result) {
		String placeholders = String.join(",", Collections.nCopies(keys.size(), "?"));
		String sql = "SELECT k, v FROM " + TABLE_NAME + " WHERE imap = ? AND k IN (" + placeholders + ")";
		try (PreparedStatement ps = duckDBResource.getConnection().prepareStatement(sql)) {
			ps.setString(1, imapName);
			int idx = 2;
			for (String key : keys) {
				ps.setString(idx++, key);
			}
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					String k = rs.getString(1);
					String json = rs.getString(2);
					if (k == null || json == null) {
						continue;
					}
					result.put(k, Document.parse(json));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("LoadAll from DuckDB failed, imap: " + imapName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	public Iterable<String> loadAllKeys() {
		return null;
	}

	@Override
	public Iterable<Document> iterator() {
		if (!checkEnable() || duckDBResource == null) {
			return null;
		}
		return new DuckDBImapIterable(duckDBResource.getConnection(), imapName);
	}

	static class DuckDBImapIterable implements Iterable<Document> {
		private final Connection connection;
		private final String imap;

		DuckDBImapIterable(Connection connection, String imap) {
			this.connection = connection;
			this.imap = imap;
		}

		@Override
		public void forEach(Consumer<? super Document> action) {
			String sql = "SELECT k, v FROM " + TABLE_NAME + " WHERE imap = ?";
			try (PreparedStatement ps = connection.prepareStatement(sql)) {
				ps.setString(1, imap);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						String k = rs.getString(1);
						String json = rs.getString(2);
						if (k == null || json == null) {
							continue;
						}
						Document value = Document.parse(json);
						Document doc = new Document("key", k).append(VALUE_KEY, value);
						action.accept(doc);
					}
				}
			} catch (SQLException e) {
				throw new RuntimeException("Iterate DuckDBIMap failed, imap: " + imap, e);
			}
		}

		@Override
		public Iterator<Document> iterator() {
			return new DuckDBImapIterator(connection, imap);
		}
	}

	static class DuckDBImapIterator implements Iterator<Document> {
		private final PreparedStatement preparedStatement;
		private final ResultSet resultSet;
		private boolean hasNextCalled;
		private boolean hasNext;

		DuckDBImapIterator(Connection connection, String imap) {
			try {
				this.preparedStatement = connection.prepareStatement("SELECT k, v FROM " + TABLE_NAME + " WHERE imap = ?");
				this.preparedStatement.setString(1, imap);
				this.resultSet = preparedStatement.executeQuery();
			} catch (SQLException e) {
				throw new RuntimeException("Create DuckDBIMap iterator failed, imap: " + imap, e);
			}
		}

		@Override
		public boolean hasNext() {
			if (hasNextCalled) {
				return hasNext;
			}
			try {
				hasNext = resultSet.next();
				hasNextCalled = true;
				if (!hasNext) {
					close();
				}
				return hasNext;
			} catch (SQLException e) {
				close();
				throw new RuntimeException(e);
			}
		}

		@Override
		public Document next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			try {
				String k = resultSet.getString(1);
				String json = resultSet.getString(2);
				hasNextCalled = false;
				if (k == null || json == null) {
					return new Document();
				}
				return new Document("key", k).append(VALUE_KEY, Document.parse(json));
			} catch (SQLException e) {
				close();
				throw new RuntimeException(e);
			}
		}

		private void close() {
			try {
				resultSet.close();
			} catch (SQLException ignored) {
			}
			try {
				preparedStatement.close();
			} catch (SQLException ignored) {
			}
		}
	}

	@Override
	public boolean isEmpty() {
		if (!checkEnable() || duckDBResource == null) {
			return true;
		}
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT 1 FROM " + TABLE_NAME + " WHERE imap = ? LIMIT 1")) {
			ps.setString(1, imapName);
			try (ResultSet rs = ps.executeQuery()) {
				return !rs.next();
			}
		} catch (SQLException e) {
			throw new RuntimeException("Check DuckDBIMap isEmpty failed, imap: " + imapName + ", config: " + persistenceDuckDBConfig, e);
		}
	}

	@Override
	public synchronized Map<String, Object> getStatistics() {
		if (!checkEnable() || duckDBResource == null) {
			return null;
		}
		Map<String, Object> statistics = new HashMap<>();
		try (PreparedStatement ps = duckDBResource.getConnection()
				.prepareStatement("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE imap = ?")) {
			ps.setString(1, imapName);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					statistics.put("count", rs.getLong(1));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Get DuckDBIMap statistics failed, imap: " + imapName + ", config: " + persistenceDuckDBConfig, e);
		}
		statistics.put("uri", persistenceDuckDBConfig.uriInfo());
		statistics.put("mode", persistenceDuckDBConfig.getStorageMode().name());
		statistics.put("inMemory", persistenceDuckDBConfig.isInMemory());
		return statistics;
	}
}
