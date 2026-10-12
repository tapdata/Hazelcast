package com.hazelcast.persistence;

import com.mongodb.MongoCommandException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

/**
 * Statistics for callers that do not consume sharding metadata.
 * Aggregation results expose only count, size, storageSize and avgObjSize;
 * other legacy diagnostic fields are intentionally not synthesized across shards.
 * Schema discovery and sharding detection must keep their existing command path.
 */
public final class MongoCollectionStatistics {
    private int[] version;
    private Object versionScope;

    public Document read(MongoDatabase database, String collection) {
        return read(database, collection, database);
    }

    public Document read(MongoDatabase database, String collection, Object connectionScope) {
        if (!supportsAggregation(database, connectionScope)) {
            return database.runCommand(new Document("collStats", collection));
        }
        Document result = null;
        long count = 0;
        long size = 0;
        long storageSize = 0;
        int rows = 0;
        try (MongoCursor<Document> cursor = database.getCollection(collection).withReadPreference(ReadPreference.primary())
                .aggregate(Arrays.asList(
                        new Document("$collStats", new Document("storageStats", new Document())),
                        new Document("$project", new Document("storageStats.count", 1)
                                .append("storageStats.size", 1).append("storageStats.storageSize", 1)
                                .append("storageStats.avgObjSize", 1).append("_id", 0))))
                .iterator()) {
            while (cursor.hasNext()) {
                Document stats = cursor.next().get("storageStats", Document.class);
                if (stats == null) {
                    throw new IllegalStateException("MongoDB collection statistics are missing");
                }
                if (result == null) {
                    result = new Document("count", stats.get("count"))
                            .append("size", stats.get("size"))
                            .append("storageSize", stats.get("storageSize"))
                            .append("avgObjSize", stats.get("avgObjSize"));
                }
                count += number(stats, "count");
                size += number(stats, "size");
                storageSize += number(stats, "storageSize");
                rows++;
            }
        } catch (MongoCommandException e) {
            // Legacy collStats can return zero statistics for a missing collection.
            // Preserve that server-specific contract instead of inventing a result.
            if (e.getErrorCode() == 26) {
                return database.runCommand(new Document("collStats", collection));
            }
            throw e;
        }
        if (result == null) {
            throw new IllegalStateException("MongoDB collection statistics are empty");
        }
        if (rows > 1) {
            result.put("count", count);
            result.put("size", size);
            result.put("storageSize", storageSize);
            result.put("avgObjSize", count == 0 ? 0D : (double) size / count);
        }
        return result;
    }

    /** Clear connection-bound state when its owning storage resource is released. */
    public synchronized void reset() {
        version = null;
        versionScope = null;
    }

    /** Reuse the same discovery for index initialization and subsequent statistics. */
    public synchronized boolean supportsIndexCommitQuorum(MongoDatabase database, Object connectionScope) {
        int[] parsed = serverVersion(database, connectionScope);
        return parsed != null && (parsed[0] > 4 || (parsed[0] == 4 && parsed[1] >= 4));
    }

    private synchronized boolean supportsAggregation(MongoDatabase database, Object connectionScope) {
        int[] parsed = serverVersion(database, connectionScope);
        return parsed != null && parsed[0] >= 6;
    }

    private int[] serverVersion(MongoDatabase database, Object connectionScope) {
        if (version != null && versionScope == connectionScope) {
            return version;
        }
        String value;
        try {
            Object raw = database.runCommand(new Document("buildInfo", 1)).get("version");
            value = raw instanceof String ? (String) raw : null;
        } catch (MongoCommandException rejected) {
            // A server refusal must not introduce a new permission prerequisite.
            // Transport failures propagate instead of causing a second timed-out query.
            return null;
        }
        if (value == null || value.isEmpty() || !Character.isDigit(value.charAt(0))) {
            return null;
        }
        version = MongodbUtil.parseMajorMinor(value);
        versionScope = connectionScope;
        return version;
    }

    private static long number(Document stats, String field) {
        Number value = (Number) stats.get(field);
        return value == null ? 0L : value.longValue();
    }
}

