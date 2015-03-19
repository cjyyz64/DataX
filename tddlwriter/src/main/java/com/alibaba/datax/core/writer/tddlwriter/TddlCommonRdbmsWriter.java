package com.alibaba.datax.core.writer.tddlwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Date: 15/3/19 下午4:05
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class TddlCommonRdbmsWriter extends CommonRdbmsWriter {

    public static class Task extends CommonRdbmsWriter.Task {

        public Task(DataBaseType dataBaseType) {
            super(dataBaseType);
        }

        @Override
        public PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record) throws SQLException {
            for (int i = 0; i < super.columnNumber; i++) {
                int columnSqltype = resultSetMetaData.getMiddle().get(i);
                switch (columnSqltype) {
                    case Types.INTEGER:
                    case Types.DECIMAL:
                    case Types.BIGINT:
                        preparedStatement.setLong(i + 1, record.getColumn(i)
                                .asLong());
                        break;
                    case 10003:
                        java.sql.Timestamp sqlTimestamp = null;
                        java.util.Date utilDate;
                        try {
                            utilDate = record.getColumn(i).asDate();
                        } catch (DataXException e) {
                            throw new SQLException(String.format(
                                    "TIMESTAMP 类型转换错误：[%s]", record.getColumn(i)));
                        }

                        if (null != utilDate) {
                            sqlTimestamp = new java.sql.Timestamp(utilDate.getTime());
                        }
                        preparedStatement.setTimestamp(i + 1, sqlTimestamp);
                        break;
                    default:
                        preparedStatement = super.fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, record.getColumn(i));
                }
            }
            return preparedStatement;
        }
    }

}
