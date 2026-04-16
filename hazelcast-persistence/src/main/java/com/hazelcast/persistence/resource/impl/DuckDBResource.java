package com.hazelcast.persistence.resource.impl;

import com.hazelcast.persistence.config.PersistenceDuckDBConfig;
import com.hazelcast.persistence.resource.ExternalResource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DuckDBResource extends ExternalResource<PersistenceDuckDBConfig> {
	private Connection connection;
	private String jdbcUrl;

	static {
		try {
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException ignored) {
		}
	}

	@Override
	public void doInit(PersistenceDuckDBConfig persistenceDuckDBConfig) {
		super.doInit(persistenceDuckDBConfig);
		this.jdbcUrl = persistenceDuckDBConfig.jdbcUrl();
		this.connection = openConnection(this.jdbcUrl);
	}

	private static Connection openConnection(String jdbcUrl) {
		try {
			Connection connection = DriverManager.getConnection(jdbcUrl);
			connection.setAutoCommit(true);
			return connection;
		} catch (SQLException e) {
			throw new RuntimeException("Open DuckDB connection failed, jdbcUrl: " + jdbcUrl, e);
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public synchronized void reInit(PersistenceDuckDBConfig persistenceDuckDBConfig) {
		String newJdbcUrl = persistenceDuckDBConfig.jdbcUrl();
		if (newJdbcUrl.equals(this.jdbcUrl)) {
			this.persistenceStorageAbstractConfig = persistenceDuckDBConfig;
			return;
		}
		try {
			close();
		} catch (IOException ignored) {
		}
		this.jdbcUrl = newJdbcUrl;
		this.connection = openConnection(this.jdbcUrl);
		this.persistenceStorageAbstractConfig = persistenceDuckDBConfig;
	}

	@Override
	public synchronized void close() throws IOException {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new IOException(e);
			} finally {
				connection = null;
			}
		}
	}
}
