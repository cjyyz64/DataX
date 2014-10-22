package com.alibaba.datax.plugin.rdbms.reader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.SlavePluginCollector;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

public class ResultSetReadProxy {

    public static void transportOneRecord(RecordSender recordSender, ResultSet rs,
                                          ResultSetMetaData metaData, int columnNumber,
                                          SlavePluginCollector slavePluginCollector) {
        Record record = recordSender.createRecord();

        try {
            for (int i = 1; i <= columnNumber; i++) {
                switch (metaData.getColumnType(i)) {

                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.CLOB:
                    case Types.NCLOB:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;

                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        record.addColumn(new LongColumn(rs.getLong(i)));
                        break;

                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        record.addColumn(new DoubleColumn(rs.getDouble(i)));
                        break;

                    case Types.TIME:
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;

                    case Types.DATE:
                        record.addColumn(new DateColumn(rs.getDate(i)));
                        break;

                    case Types.TIMESTAMP:
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;

                    // TODO  可能要把 bit 删除
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;

                    // TODO 添加BASIC_MESSAGE
                    default:
                        throw new Exception(
                                String.format(
                                        "Unsupported Mysql Data Type. ColumnName:[%s], ColumnType:[%s], ColumnClassName:[%s].",
                                        metaData.getColumnName(i),
                                        metaData.getColumnType(i),
                                        metaData.getColumnClassName(i)));
                }

            }
            recordSender.sendToWriter(record);
        } catch (Exception e) {
            slavePluginCollector.collectDirtyRecord(record, e);
        }
    }
}
