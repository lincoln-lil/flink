package org.apache.flink.table.functions.utils.hbase;

public class CachePolicy {

    Strategy cacheStrategy = null;

    int maxRowCount;

    int ttlSecond;

    long maxCacheMemBytes;

    enum Strategy {
        /**
         * no caching
         */
        NONE,

        /**
         * LRU caching with row count limitation
         */
        LRU_WITH_ROW_LIMIT,

        /**
         * TTL support based on LRU caching with row count limitation
         */
        LRU_WITH_ROW_LIMIT_WITH_TTL,

        /**
         * LRU caching with a reasonable maximum size for memory restriction
         */
        LRU_WITH_MEM_LIMIT
    }

    private CachePolicy(Strategy cacheStrategy, int maxRowCount, int ttlSecond, long
            maxCacheMemBytes){
        this.cacheStrategy = cacheStrategy;
        this.maxRowCount = maxRowCount;
        this.ttlSecond = ttlSecond;
        this.maxCacheMemBytes = maxCacheMemBytes;
    }

    public static CachePolicy none(){
        return new CachePolicy(Strategy.NONE, -1, -1, -1);
    }

    public static CachePolicy rowCountBasedLRU(int maxRowCount){
        return new CachePolicy(Strategy.LRU_WITH_ROW_LIMIT, maxRowCount, -1, -1);
    }

    public static CachePolicy rowCountWithTtlLRU(int maxRowCount, int ttlSecond){
        return new CachePolicy(Strategy.LRU_WITH_ROW_LIMIT_WITH_TTL, maxRowCount, ttlSecond, -1);
    }

    public static CachePolicy memoryBasedLRU(long maxCacheMemBytes){
        return new CachePolicy(Strategy.LRU_WITH_MEM_LIMIT, -1, -1, maxCacheMemBytes);
    }
}
