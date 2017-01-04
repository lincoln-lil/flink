package org.apache.flink.table.functions.utils.hbase;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.flink.table.sources.HBaseTableSource;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.client.AsyncGetCallback;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

public class JoinHTableAsyncGetCallback extends AsyncGetCallback {
    AsyncCollector collector = null;
    HBaseTableSource tableSource = null;
    JoinRelType joinType = null;
    int sourceKeyIdx = 0;
    Row leftRow = null;
    ResultParser resultParser = null;

    public JoinHTableAsyncGetCallback(
            AsyncCollector collector, byte[] row, HBaseTableSource
            tableSource, JoinRelType joinType, int sourceKeyIdx, Row leftRow, ResultParser
                    resultParser) {
        super(row);
        this.leftRow = leftRow;
        this.collector = collector;
        this.tableSource = tableSource;
        this.joinType = joinType;
        this.sourceKeyIdx = sourceKeyIdx;
        this.resultParser = resultParser;
    }

    @Override
    public void processResult(Result result) {
        // parse result
        FieldMap row = resultParser.parseResult(result);

        // join
        Row resRow = new Row(leftRow.getArity() + tableSource.getNumberOfFields());
        if (row == null && joinType == JoinRelType.INNER) {
            // filter this record, TODO add metric info

        } else {
            // copy left row to resRow
            for (int idx = 0; idx < leftRow.getArity(); idx++) {
                resRow.setField(idx, leftRow.getField(idx));
            }
            // copy rowkey column, it can be removed when support projection push down
            resRow.setField(leftRow.getArity(), leftRow.getField(sourceKeyIdx));
            for (int idx = 1; idx < tableSource.getNumberOfFields(); idx++) {
                resRow.setField(
                        leftRow.getArity() + idx,
                        (row == null ? null : row.get(tableSource.getFieldsNames()[idx])));
            }
            // collect data
            List<Row> res = new ArrayList(1);
            res.add(resRow);
            collector.collect(res);
        }
    }

    @Override
    public void processError(Throwable exception) {
        //print error stack, and throw a RuntimeException triggering a fail over since could not
        //retry here.
        exception.printStackTrace(System.err);
        throw new RuntimeException(exception);
    }

}
