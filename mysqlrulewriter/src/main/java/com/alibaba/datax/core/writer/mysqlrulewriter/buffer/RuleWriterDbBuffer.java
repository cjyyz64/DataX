package com.alibaba.datax.core.writer.mysqlrulewriter.buffer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 15/5/6 下午7:57
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class RuleWriterDbBuffer {

    private String jdbcUrl;
    private Map<String, List<Record>> tableBuffer = new HashMap<String, List<Record>>();
    private Connection connection;

    public Connection initConnection(Configuration writerSliceConfig, String userName, String password) {
        String BASIC_MESSAGE = String.format("jdbcUrl:[%s]", this.jdbcUrl);
        connection = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl, userName, password);
        DBUtil.dealWithSessionConfig(connection, writerSliceConfig, DataBaseType.MySql, BASIC_MESSAGE);
        return connection;
    }

    public void initTableBuffer(List<String> tableList) {
        for(String table : tableList) {
            tableBuffer.put(table, new ArrayList<Record>());
        }
    }

    public void addRecord(Record record, String tableName) {
        List<Record> recordList = tableBuffer.get(tableName);
        if(recordList == null) {
            throw new RuntimeException("该table不存在，tableName:" + tableName + String.format(",jdbcUrl:[%s]", this.jdbcUrl));
        }
        recordList.add(record);
    }

    public Map<String, List<Record>> getTableBuffer() {
        return tableBuffer;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public Connection getConnection() {
        return connection;
    }

}
