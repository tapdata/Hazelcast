package com.hazelcast.persistence.store;

import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBInstance {
	private static ConcurrentHashMap<String, RocksDBRes> rocksDBMap;

	static {
		rocksDBMap = new ConcurrentHashMap<>();
	}

	private RocksDBInstance() {
	}

	public static RocksDB getInstance(String path) {
		assert StringUtils.isNotBlank(path);
		String lock = getLock(path);
		synchronized (lock.intern()) {
			if (!rocksDBMap.containsKey(path)) {
				Options options = new Options()
						.setEnableWriteThreadAdaptiveYield(true)
						.setAllowConcurrentMemtableWrite(true)
						.setCreateIfMissing(true);
				try {
					RocksDB rocksDB = RocksDB.open(options, path);
					RocksDBRes rocksDBRes = new RocksDBRes(rocksDB);
					rocksDBMap.put(path, rocksDBRes);
				} catch (RocksDBException e) {
					throw new RuntimeException("Create RocksDB instance failed, path: " + path, e);
				}
			} else {
				rocksDBMap.get(path).incrementUsageCountAndGet();
			}
			return rocksDBMap.get(path).getRocksDB();
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
				rocksDBRes.getRocksDB().close();
				rocksDBMap.remove(path);
			}
		}
	}

	private static String getLock(String path) {
		return RocksDBInstance.class.getName().replaceAll("\\.", "_") + "_" + path;
	}

	private static class RocksDBRes {
		private RocksDB rocksDB;
		private AtomicInteger usageCount;

		public RocksDBRes(RocksDB rocksDB) {
			this.rocksDB = rocksDB;
			this.usageCount = new AtomicInteger(1);
		}

		public RocksDB getRocksDB() {
			return rocksDB;
		}

		public int incrementUsageCountAndGet() {
			return usageCount.incrementAndGet();
		}

		public int decrementUsageCountAndGet() {
			return usageCount.decrementAndGet();
		}
	}
}