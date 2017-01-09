package org.apache.flink.table.functions.utils.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

public class CacheFactory {
    private static final Log LOG = LogFactory.getLog(CacheFactory.class);
    // assume averagely 1KB per cache data, this maximum cache size will cost approximately
    // 100~200MB memory in java
    private static final int MAXIMUM_CACHE_SIZE = 100000;
    private static final int ONE_YEAE_TIME_MILLISECOND = 365 * 24 * 60 * 60 * 1000;

    private static Map<String, LRUCache<String, FieldMap>> cacheMap = new HashMap<>();

    public static synchronized LRUCache<String, FieldMap> getCache(
            String tableName, CachePolicy cachePolicy) {
        if (CachePolicy.Strategy.LRU_WITH_ROW_LIMIT == cachePolicy.cacheStrategy) {
            return createLRUCache(tableName, cachePolicy.maxRowCount > MAXIMUM_CACHE_SIZE ?
                    MAXIMUM_CACHE_SIZE : cachePolicy.maxRowCount);
        } else if (CachePolicy.Strategy.LRU_WITH_ROW_LIMIT_WITH_TTL == cachePolicy.cacheStrategy) {
            return createLRUCache(
                    tableName,
                    cachePolicy.maxRowCount > MAXIMUM_CACHE_SIZE ?
                            MAXIMUM_CACHE_SIZE : cachePolicy.maxRowCount,
                    cachePolicy.ttlSecond * 1000 > ONE_YEAE_TIME_MILLISECOND ?
                            ONE_YEAE_TIME_MILLISECOND : cachePolicy.ttlSecond * 1000);
        } else { // NONE
            return null;
        }
    }

    private static LRUCache<String, FieldMap> createLRUCache(
            String tableName,
            int cacheSize) {
        LRUCache<String, FieldMap> lruCache = cacheMap.get(tableName);
        if (lruCache == null) {
            lruCache = new LRUCache<>(cacheSize);
            cacheMap.put(tableName, lruCache);
            LOG.info("create LRUCache, tableName=" + tableName + ", cacheSize=" + cacheSize);
        }
        return lruCache;
    }

    private static LRUCache<String, FieldMap> createLRUCache(
            String tableName,
            int cacheSize,
            int cacheExpireTime) {
        LRUCache<String, FieldMap> lruCache = cacheMap.get(tableName);
        if (lruCache == null) {
            lruCache = new LRUCache<>(cacheSize, cacheExpireTime);
            cacheMap.put(tableName, lruCache);
            LOG.info("create LRUCache, tableName=" + tableName + ", cacheSize=" + cacheSize +
                     ", expireTime=" + cacheExpireTime);
        }
        return lruCache;
    }
}
