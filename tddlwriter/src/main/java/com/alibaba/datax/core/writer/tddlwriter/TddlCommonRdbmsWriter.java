package com.alibaba.datax.core.writer.tddlwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

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
        public void init(Configuration writerSliceConfig) {
            this.table = writerSliceConfig.getString(Key.TABLE);

            this.columns = writerSliceConfig.getList(Key.COLUMN, String.class);
            this.columnNumber = this.columns.size();

            emptyAsNull = writerSliceConfig.getBool(Key.EMPTY_AS_NULL, true);
            this.batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);

            writeMode = writerSliceConfig.getString(Key.WRITE_MODE, "INSERT");
            INSERT_OR_REPLACE_TEMPLATE = writerSliceConfig.getString(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK);
            this.writeRecordSql = String.format(INSERT_OR_REPLACE_TEMPLATE, this.table);
        }

        @Override
        public PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record) throws SQLException {
            java.util.Date utilDate;
            for (int i = 0; i < super.columnNumber; i++) {
                int columnSqltype = resultSetMetaData.getMiddle().get(i);
                switch (columnSqltype) {
                    case Types.INTEGER:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                    case DataType.MEDIAUMNINT_SQL_TYPE :
                        String strValue = record.getColumn(i).asString();
                        if (emptyAsNull && StringUtils.isBlank(strValue)) {
                            preparedStatement.setString(i + 1, null);
                        } else {
                            preparedStatement.setLong(i + 1, record.getColumn(i).asLong());
                        }
                        break;
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        strValue = record.getColumn(i).asString();
                        if (emptyAsNull && StringUtils.isBlank(strValue)) {
                            preparedStatement.setString(i + 1, null);
                        } else {
                            preparedStatement.setBigDecimal(i + 1, record.getColumn(i).asBigDecimal());
                        }
                        break;
                    case DataType.DATETIME_SQL_TYPE:
                        preparedStatement = super.fillPreparedStatementColumnType(preparedStatement, i, Types.TIMESTAMP, record.getColumn(i));
                        break;
                    case Types.BOOLEAN:
                        strValue = record.getColumn(i).asString();
                        if (emptyAsNull && StringUtils.isBlank(strValue)) {
                            preparedStatement.setString(i + 1, null);
                        } else {
                            preparedStatement.setBoolean(i + 1, record.getColumn(i).asBoolean());
                        }
                        break;
                    case DataType.YEAR_SQL_TYPE:
                        if (this.resultSetMetaData.getRight().get(i)
                                .equalsIgnoreCase("YearType")) {
                            if (record.getColumn(i).asBigInteger() == null) {
                                preparedStatement.setString(i + 1, null);
                            } else {
                                preparedStatement.setInt(i + 1, record.getColumn(i).asBigInteger().intValue());
                            }
                        } else {
                            java.sql.Date sqlDate = null;
                            try {
                                utilDate = record.getColumn(i).asDate();
                            } catch (DataXException e) {
                                throw new SQLException(String.format(
                                        "Date 类型转换错误：[%s]", record.getColumn(i)));
                            }

                            if (null != utilDate) {
                                sqlDate = new java.sql.Date(utilDate.getTime());
                            }
                            preparedStatement.setDate(i + 1, sqlDate);
                        }
                        break;
                    default:
                        preparedStatement = super.fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, record.getColumn(i));
                }
            }
            return preparedStatement;
        }

        //跨库事务不支持，不能将autoCommit设置为false
        @Override
        protected void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection
                        .prepareStatement(this.writeRecordSql);

                for (Record record : buffer) {
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            } catch (TddlNestableRuntimeException e) {
                LOG.warn("插入失败. 存在脏数据. 因为:" + e.getMessage());
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.WRITE_DATA_ERROR, e);
            } finally {
                DBUtil.closeDBResources(preparedStatement, null);
            }
        }
    }

}
