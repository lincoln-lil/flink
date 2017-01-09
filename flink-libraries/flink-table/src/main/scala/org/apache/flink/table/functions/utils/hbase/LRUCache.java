package org.apache.flink.table.functions.utils.hbase;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * LRU Cache support TTL
 s*
 * @param <K> key
 * @param <V> value
 */
public class LRUCache<K, V> {
    private Cache<K, V> cache = null;

    public LRUCache(final int maxRowCount) {
        this.cache = CacheBuilder.newBuilder().maximumSize(maxRowCount).build();
    }

    public LRUCache(final int maxRowCount, final int ttlSecond) {
        this.cache = CacheBuilder.newBuilder()
                                 .maximumSize(maxRowCount)
                                 .expireAfterWrite(ttlSecond, TimeUnit.SECONDS)
                                 .build();
    }

    public synchronized V get(K key) {
        return this.cache.getIfPresent(key);
    }

    public synchronized void put(K key, V val) {
        this.cache.put(key, val);
    }

    public synchronized void remove(K key) {
        this.cache.invalidate(key);
    }

    public long size() {
        return this.cache.size();
    }

    public double getHitRate() {
        return this.cache.stats().hitRate();
    }
}