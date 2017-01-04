package org.apache.flink.table.functions.utils.hbase;

import org.apache.hadoop.hbase.client.Result;

public interface ResultParser {

    /**
     * parse @org.apache.hadoop.hbase.client.Result to FieldMap
     * @param result
     * @return
     */
    FieldMap parseResult(Result result);
}
