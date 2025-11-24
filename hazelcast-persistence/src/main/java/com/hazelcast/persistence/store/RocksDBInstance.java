package com.hazelcast.persistence.store;

import org.apache.commons.lang3.StringUtils;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBInstance {
	private static ConcurrentHashMap<String, RocksDBRes> rocksDBMap;

	static {
		rocksDBMap = new ConcurrentHashMap<>();
	}

	private RocksDBInstance() {
	}


	public static ColumnFamilyHandle getColumnFamilyHandle(String path, String columnFamilyName) {
		String lock = getLock(path);
		synchronized (lock.intern()) {
			RocksDBRes rocksDBRes = rocksDBMap.get(path);
			if (rocksDBRes == null) {
				initRocksDB(path);
				rocksDBRes = rocksDBMap.get(path);
			}

			return rocksDBRes.getOrCreateColumnFamily(columnFamilyName);
		}
	}

	public static RocksDB getInstance(String path) {
		assert StringUtils.isNotBlank(path);
		String lock = getLock(path);
		synchronized (lock.intern()) {
			if (!rocksDBMap.containsKey(path)) {
				initRocksDB(path);
			}
			return rocksDBMap.get(path).getRocksDB();
		}
	}

	private static void initRocksDB(String path) {

		DBOptions dbOptions = new DBOptions()
				.setEnableWriteThreadAdaptiveYield(true)
				.setAllowConcurrentMemtableWrite(true)
				.setCreateIfMissing(true)
				.setCreateMissingColumnFamilies(true);

		ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();

		try {
			List<byte[]> existingCfNames;
			try {
				existingCfNames = RocksDB.listColumnFamilies(new Options(), path);
			} catch (RocksDBException e) {
				existingCfNames = new ArrayList<>();
			}

			if (existingCfNames.isEmpty()) {
				existingCfNames = new ArrayList<>();
				existingCfNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
			}

			List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
			for (byte[] cfName : existingCfNames) {
				cfDescriptors.add(new ColumnFamilyDescriptor(cfName, cfOptions));
			}

			List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
			RocksDB rocksDB = RocksDB.open(dbOptions, path, cfDescriptors, cfHandles);

			RocksDBRes rocksDBRes = new RocksDBRes(rocksDB, dbOptions, cfOptions, cfHandles, cfDescriptors, path);
			rocksDBMap.put(path, rocksDBRes);
		} catch (RocksDBException e) {
			throw new RuntimeException("Create RocksDB instance failed, path: " + path, e);
		}
	}

	public static void close(String path) {
		assert StringUtils.isNotBlank(path);
		String lock = getLock(path);
		synchronized (lock.intern()) {
			if (!rocksDBMap.containsKey(path)) {
				return;
			}
			RocksDBRes rocksDBRes = rocksDBMap.get(path);
			if (rocksDBRes.decrementUsageCountAndGet() <= 0) {
				rocksDBRes.close();
				rocksDBMap.remove(path);
			}
		}
	}

	public static void removeColumnFamily(String path, String columnFamilyName) {
		String lock = getLock(path);
		synchronized (lock.intern()) {
			if (!rocksDBMap.containsKey(path)) {
				return;
			}
			RocksDBRes rocksDBRes = rocksDBMap.get(path);
			rocksDBRes.removeColumnFamily(columnFamilyName);
		}
	}

	private static String getLock(String path) {
		return RocksDBInstance.class.getName().replaceAll("\\.", "_") + "_" + path;
	}

	private static class RocksDBRes {
		private RocksDB rocksDB;
		private DBOptions dbOptions;
		private ColumnFamilyOptions cfOptions;
		private AtomicInteger usageCount;
		private String dbPath;

		private ConcurrentHashMap<String, ColumnFamilyHandle> columnFamilyHandles;

		public RocksDBRes(RocksDB rocksDB, DBOptions dbOptions, ColumnFamilyOptions cfOptions,
						  List<ColumnFamilyHandle> cfHandles,
		                  List<ColumnFamilyDescriptor> cfDescriptors, String dbPath) {
			this.rocksDB = rocksDB;
			this.dbOptions = dbOptions;
			this.cfOptions = cfOptions;
			this.usageCount = new AtomicInteger(0);
			this.dbPath = dbPath;
			this.columnFamilyHandles = new ConcurrentHashMap<>();

			for (int i = 0; i < cfHandles.size(); i++) {
				String cfName = new String(cfDescriptors.get(i).getName(), StandardCharsets.UTF_8);
				columnFamilyHandles.put(cfName, cfHandles.get(i));
			}
		}

		public RocksDB getRocksDB() {
			return rocksDB;
		}

		public ColumnFamilyHandle getOrCreateColumnFamily(String columnFamilyName) {
			ColumnFamilyHandle handle = columnFamilyHandles.get(columnFamilyName);
			if (handle != null) {
				incrementUsageCountAndGet();
				return handle;
			}

			synchronized (this) {
				handle = columnFamilyHandles.get(columnFamilyName);
				if (handle != null) {
					incrementUsageCountAndGet();
					return handle;
				}

				try {
					ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
							columnFamilyName.getBytes(StandardCharsets.UTF_8),
							cfOptions
					);
					handle = rocksDB.createColumnFamily(cfDescriptor);
					columnFamilyHandles.put(columnFamilyName, handle);
					incrementUsageCountAndGet();
					return handle;
				} catch (RocksDBException e) {
					throw new RuntimeException("Create column family failed: " + columnFamilyName, e);
				}
			}
		}

		public void removeColumnFamily(String columnFamilyName) {
			if (columnFamilyHandles.containsKey(columnFamilyName)) {
				ColumnFamilyHandle handle = columnFamilyHandles.remove(columnFamilyName);
				handle.close();
			}
		}


		public int incrementUsageCountAndGet() {
			return usageCount.incrementAndGet();
		}

		public int decrementUsageCountAndGet() {
			return usageCount.decrementAndGet();
		}

		public void close() {
			for (ColumnFamilyHandle handle : columnFamilyHandles.values()) {
				if (handle != null) {
					handle.close();
				}
			}
			columnFamilyHandles.clear();

			if (rocksDB != null) {
				rocksDB.close();
			}
			if (dbOptions != null) {
				dbOptions.close();
			}
			if (cfOptions != null) {
				cfOptions.close();
			}
		}
	}
}