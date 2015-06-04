package com.alibaba.datax.plugin.reader.mysqlreader;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Created by judy.lt on 2015/6/4.
 */
public class PreCheckTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(PreCheckTask.class);
    private String userName;
    private String password;
    private String querySql;
    private String jdbcUrl;
    private DataBaseType dataBaseType;

    public PreCheckTask(String userName,
                        String password,
                        String querySql,
                        String jdbcUrl,
                        DataBaseType dataBaseType){
        this.userName=userName;
        this.password=password;
        this.querySql=querySql;
        this.jdbcUrl= jdbcUrl;
        this.dataBaseType = dataBaseType;
    }

    @Override
    public void run() {
        Connection conn = DBUtil.getConnection(this.dataBaseType, this.jdbcUrl,
                this.userName, password);
        int fetchSize = 1;
        try {
            boolean isPassSqlParser = DBUtil.sqlValid(querySql, DataBaseType.MySql);
            if (isPassSqlParser == true){
                DBUtil.query(conn, querySql, fetchSize);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        } finally {
            DBUtil.closeDBResources(null, conn);
        }

    }
}
