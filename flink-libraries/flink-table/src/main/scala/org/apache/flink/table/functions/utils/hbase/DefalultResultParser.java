package org.apache.flink.table.functions.utils.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.NavigableMap;

public class DefalultResultParser implements ResultParser {

    public FieldMap parseResult(Result result) {
        FieldMap resultMap = new FieldMap();
        String rowKey = Bytes.toString(result.getRow());
        if (rowKey == null) {
            return resultMap;
        }

        NavigableMap<byte[], NavigableMap<byte[], byte[]>> byteMap = result.getNoVersionMap();
        for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> entry : byteMap.entrySet()) {
            String columnFamily = Bytes.toString(entry.getKey());
            for (Map.Entry<byte[], byte[]> e : entry.getValue().entrySet()) {
                String qualifier = Bytes.toString(e.getKey());
                String value = Bytes.toString(e.getValue());
                String field = columnFamily + ":" + qualifier;
                if (value != null && value.equalsIgnoreCase("\\N")) {
                    value = "";
                }
                resultMap.put(field, value);
            }
        }
        return resultMap;
    }
}
