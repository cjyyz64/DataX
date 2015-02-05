package com.alibaba.datax.plugin.reader.hbasereader.util;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.hbasereader.ColumnType;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseColumnCell;
import com.alibaba.datax.plugin.reader.hbasereader.HbaseReaderErrorCode;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class NormalReader extends HbaseAbstractReader {
    public NormalReader(Configuration configuration) {
        super(configuration);
    }

    @Override
    public boolean fetchLine(Record record) throws Exception {
        Result result = super.getNextHbaseRow();

        if (null == result) {
            return false;
        }
        super.lastResult = result;

        try {
            byte[] hbaseColumnValue;
            String columnName;
            ColumnType columnType;

            byte[] cf;
            byte[] qualifier;

            for (HbaseColumnCell cell : super.hbaseColumnCells) {
                columnType = cell.getColumnType();
                if (cell.isConstant()) {
                    // 对常量字段的处理
                    fillRecordWithConstantValue(record, cell);
                } else {
                    // 根据列名称获取值
                    // 对 rowkey 的读取，需要考虑 isBinaryRowkey；而对普通字段的读取，isBinaryRowkey 无影响

                    columnName = cell.getColumnName();

                    if (HbaseUtil.isRowkeyColumn(columnName)) {
                        doFillRecord(result.getRow(), columnType, super.isBinaryRowkey,
                                super.encoding, cell.getDateformat(), record);
                    } else {
                        cf = cell.getCf();
                        qualifier = cell.getQualifier();
                        hbaseColumnValue = result.getValue(cf, qualifier);

                        doFillRecord(hbaseColumnValue, columnType, false, super.encoding,
                                cell.getDateformat(), record);
                    }
                }
            }
        } catch (Exception e) {
            // 注意，这里catch的异常，期望是byte数组转换失败的情况。而实际上，string的byte数组，转成整数类型是不容易报错的。但是转成double类型容易报错。

            record.setColumn(0, new StringColumn(super.bytesToString(result.getRow(), super.isBinaryRowkey, super.encoding)));

            throw e;
        }

        return true;
    }

    @Override
    public void setMaxVersions(Scan scan) {
        // do nothing
    }

    private void fillRecordWithConstantValue(Record record, HbaseColumnCell cell) throws Exception {
        String constantValue = cell.getColumnValue();
        ColumnType columnType = cell.getColumnType();
        switch (columnType) {
            case BOOLEAN:
                record.addColumn(new BoolColumn(constantValue));
                break;
            case SHORT:
            case INT:
            case LONG:
                record.addColumn(new LongColumn(constantValue));
                break;
            case BYTES:
                record.addColumn(new BytesColumn(constantValue.getBytes("utf-8")));
                break;
            case FLOAT:
            case DOUBLE:
                record.addColumn(new DoubleColumn(constantValue));
                break;
            case STRING:
                record.addColumn(new StringColumn(constantValue));
                break;
            case DATE:
                record.addColumn(new DateColumn(DateUtils.parseDate(constantValue, new String[]{cell.getDateformat()})));
                break;
            default:
                throw DataXException.asDataXException(HbaseReaderErrorCode.ILLEGAL_VALUE, "Hbasereader 不支持您配置的列类型:" + columnType);
        }
    }
}
