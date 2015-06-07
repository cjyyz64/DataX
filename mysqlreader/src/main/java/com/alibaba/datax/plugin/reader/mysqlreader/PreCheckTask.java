package com.alibaba.datax.plugin.reader.mysqlreader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by judy.lt on 2015/6/4.
 */
public class PreCheckTask implements Callable<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(PreCheckTask.class);
    private String userName;
    private String password;
    private Configuration connection;
    private DataBaseType dataBaseType;

    public PreCheckTask(String userName,
                        String password,
                        Configuration connection,
                        DataBaseType dataBaseType){
        this.connection = connection;
        this.userName=userName;
        this.password=password;
        this.dataBaseType = dataBaseType;
    }

    @Override
    public Boolean call() {
        String jdbcUrl = this.connection.getString(Key.JDBC_URL);
        List<Object> querySqls = this.connection.getList(Key.QUERY_SQL, Object.class);
        Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
                this.userName, password);
        int fetchSize = 1;
        for (int i=0;i<querySqls.size();i++){
            String querySql = querySqls.get(i).toString();
            try {
                DBUtil.sqlValid(querySql,dataBaseType);
                DBUtil.query(conn, querySql, fetchSize);
            } catch (Exception e) {
                RdbmsException.asQueryException(this.dataBaseType, e, querySql);
            } finally {
                DBUtil.closeDBResources(null, conn);
            }
        }
        return true;
    }
}
