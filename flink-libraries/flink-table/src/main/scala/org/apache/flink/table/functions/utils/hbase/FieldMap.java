package org.apache.flink.table.functions.utils.hbase;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FieldMap implements Map<String, String> {
    private Map<String, String> innerMap;

    public FieldMap() {
        this.innerMap = new HashMap<String, String>();
    }

    public Map<String, String> getDataMap() {
        return this.innerMap;
    }

    @Override
    public int size() {
        return innerMap.size();
    }

    @Override
    public boolean isEmpty() {
        return innerMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return innerMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return innerMap.containsValue(value);
    }

    @Override
    public String get(Object key) {
        return innerMap.get(key);
    }

    @Override
    public String put(String key, String value) {
        return innerMap.put(key, value);
    }

    @Override
    public String remove(Object key) {
        return innerMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        for (Entry<? extends String, ? extends String> e : m.entrySet()) {
            innerMap.put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        innerMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return innerMap.keySet();
    }

    @Override
    public Collection<String> values() {
        return innerMap.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return innerMap.entrySet();
    }
}