package com.alibaba.datax.core.writer.tddlwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.ConnectionFactory;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.taobao.tddl.client.jdbc.TDataSource;

import java.sql.Connection;

/**
 * Date: 15/3/16 下午3:33
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class TddlConnectionFactory implements ConnectionFactory {
    private String appName;

    public TddlConnectionFactory(String appName) {
        this.appName = appName;
    }

    @Override
    public Connection getConnecttion() {
        try {
            TDataSource ds = new TDataSource();
            ds.setAppName(appName);
            ds.setDynamicRule(true);
            ds.init();
            return ds.getConnection();
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    DBUtilErrorCode.CONN_DB_ERROR,
                    String.format("TDDL数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", appName), e);
        }
    }

    @Override
    public String getConnectionInfo() {
        return "appName=" + appName;
    }
}
