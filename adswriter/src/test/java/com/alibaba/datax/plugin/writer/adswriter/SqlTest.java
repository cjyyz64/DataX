package com.alibaba.datax.plugin.writer.adswriter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.writer.adswriter.insert.AdsInsertProxy;
import com.alibaba.datax.plugin.writer.adswriter.insert.AdsInsertUtil;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import com.mysql.jdbc.JDBC4PreparedStatement;

public class SqlTest {
    private Configuration conf = Configuration
            .from("{'table':'datax_bvt_all_types','url':'ads-demo-3d3dd9de.cn-hangzhou-1.ads.aliyuncs.com:3029','username':'uMbv1SoSUFqQ4DfR','password':'aikZ9GSzipW6KFT8RvSbak42fpymlI','schema':'ads_demo'}");

    @Test
    public void testAdsInsert() {
        try {
            Connection conn = AdsInsertUtil.getAdsConnect(conf);
            PreparedStatement stat = conn
                    .prepareStatement("insert into pre_post_sql_batch_bvt (ID, NAME) values (?,?)");
            stat.setString(1, "11112");
            stat.setString(2, "''`[]'yiixao'#");
            System.out.println(((JDBC4PreparedStatement) stat).asSql());
            Assert.assertTrue(((JDBC4PreparedStatement) stat)
                    .asSql()
                    .equals("insert into pre_post_sql_batch_bvt (ID, NAME) values ('11112','\\'\\'`[]\\'yiixao\\'#')"));

            stat.setString(1, "11112");
            stat.setLong(2, 333);
            System.out.println(((JDBC4PreparedStatement) stat).asSql());
            Assert.assertTrue(((JDBC4PreparedStatement) stat)
                    .asSql()
                    .equals("insert into pre_post_sql_batch_bvt (ID, NAME) values ('11112',333)"));

            stat.setFloat(1, 1024.1024f);
            stat.setTime(2, Time.valueOf("12:12:12"));
            System.out.println(((JDBC4PreparedStatement) stat).asSql());

            // to do warn
            stat.setString(1, "11112\u0000");
            ArrayList<String> data = new ArrayList<String>();
            data.add("abc");
            stat.setObject(2, data);
            System.out.println(((JDBC4PreparedStatement) stat).asSql());

            System.out.println("==============");

            Connection mysql = DBUtil.getConnection(DataBaseType.MySql,
                    "jdbc:mysql://10.101.83.3:3306/cdp", "root", "root");
            String mysqlQuery = "select 'abc', name from ssss";
            Statement mysqlStat = mysql.createStatement();
            ResultSet resultSet = mysqlStat.executeQuery(mysqlQuery);
            while (resultSet.next()) {
                String first = resultSet.getString(1);
                String second = resultSet.getString(2);
                stat.setString(1, first);
                stat.setString(2, second);
                System.out.println(first + ":" + first.length());
                System.out.println(second + ":" + second.length());
                String sql = ((JDBC4PreparedStatement) stat).asSql();
                System.out.println(sql);

                // Statement stat2 = conn.createStatement();
                // boolean result2 = stat2.execute(sql);
                // System.out.println(result2);
                Statement stat3 = conn.createStatement();
                int result3 = stat3.executeUpdate(sql);
                System.out.println(result3);
                Statement stat4 = conn.createStatement();
                StringBuilder sb = new StringBuilder();
                sb.append("insert into pre_post_sql_batch_bvt (ID, NAME) values ('");
                sb.append(first + UUID.randomUUID().toString());
                sb.append("','");
                sb.append(second);
                sb.append(")");
                System.out.println(sb + ":" + sb.length());
                int result4 = stat4.executeUpdate(sb.toString());
                System.out.println(result4);
            }
            /*
             * stat.setString(1, "datax"); stat.setString(2,
             * "11112\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0041"
             * ); System.out.println(((JDBC4PreparedStatement) stat).asSql());
             * String sql = ((JDBC4PreparedStatement) stat).asSql(); Statement
             * stat2 = conn.createStatement(); boolean result2 =
             * stat2.execute(sql); System.out.println(result2);
             * 
             * Statement stat3 = conn.createStatement(); int result3 =
             * stat3.executeUpdate(sql); System.out.println(result3);
             */
            stat.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAdsInsertProxy() throws Exception {
        TaskPluginCollector collector = new TaskPluginCollector() {
            @Override
            public void collectDirtyRecord(Record dirtyRecord, Throwable t,
                    String errorMessage) {
                System.out.println("=======捕捉到脏数据,record=" + dirtyRecord
                        + ",e=" + t + ",errorMsg=" + errorMessage);
            }

            @Override
            public void collectMessage(String key, String value) {
                System.out.println("======key=" + key + ",value=" + value);
            }
        };
        List<String> columns = Arrays.asList("col1", "col2", "col3", "col4",
                "col5", "col6", "col7", "col8", "col9", "col10", "col11",
                "col12", "col13");
        AdsInsertProxy proxy = new AdsInsertProxy("datax_bvt_all_types",
                columns, conf, collector);
        Connection conn = AdsInsertUtil.getAdsConnect(conf);
        Record record = new Record() {
            private String[] columns = new String[] { "1", "true", "3", "4",
                    "5", "6", "7.2", "8.2", null, null, null, "cdp",
                    "datax''''`[]" };

            @Override
            public void setColumn(int i, Column column) {
            }

            @Override
            public int getMemorySize() {
                return 0;
            }

            @Override
            public int getColumnNumber() {
                return 0;
            }

            @Override
            public Column getColumn(int i) {
                return new StringColumn(this.columns[i]);
            }

            @Override
            public int getByteSize() {
                return 0;
            }

            @Override
            public void addColumn(Column column) {
            }
        };

        Field resultSetMetaData = proxy.getClass().getDeclaredField(
                "resultSetMetaData");
        resultSetMetaData.setAccessible(true);
        resultSetMetaData.set(proxy,
                AdsInsertUtil.getColumnMetaData(conf, columns));

        Method generateInsertSql = proxy.getClass().getDeclaredMethod(
                "generateInsertSql", Connection.class, Record.class);
        generateInsertSql.setAccessible(true);
        Object resultSql = generateInsertSql.invoke(proxy, conn, record);
        System.out.println(resultSql);
        Assert.assertTrue("insert into datax_bvt_all_types(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) values(1,1,3,4,5,6,7.2,8.2,null,null,null,'cdp','datax\\'\\'\\'\\'`[]')"
                .equals(resultSql));

        List<Record> buffer = Arrays.asList(record, record, record, record,
                record);
        Method appendInsertSqlValues = proxy.getClass().getDeclaredMethod(
                "appendInsertSqlValues", Connection.class, Record.class,
                StringBuilder.class);
        appendInsertSqlValues.setAccessible(true);
        int bufferSize = buffer.size();
        StringBuilder sqlSb = new StringBuilder();
        sqlSb.append(generateInsertSql.invoke(proxy, conn, record));
        conf.set(Key.BATCH_SIZE, 2);
        for (int i = 1; i < bufferSize; i++) {
            Record each = buffer.get(i);
            appendInsertSqlValues.invoke(proxy, conn, each, sqlSb);
        }
        System.out.println(sqlSb);
        Assert.assertTrue("insert into datax_bvt_all_types(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13) values(1,1,3,4,5,6,7.2,8.2,null,null,null,'cdp','datax\\'\\'\\'\\'`[]'),(1,1,3,4,5,6,7.2,8.2,null,null,null,'cdp','datax\\'\\'\\'\\'`[]'),(1,1,3,4,5,6,7.2,8.2,null,null,null,'cdp','datax\\'\\'\\'\\'`[]'),(1,1,3,4,5,6,7.2,8.2,null,null,null,'cdp','datax\\'\\'\\'\\'`[]'),(1,1,3,4,5,6,7.2,8.2,null,null,null,'cdp','datax\\'\\'\\'\\'`[]')"
                .equals(sqlSb.toString()));
    }

    @Test
    public void testPrepareJdbcUrl() {
        String jdbcUrl = AdsUtil.prepareJdbcUrl("127.0.0.1", "database",
                3600000L, "");
        System.out.println(jdbcUrl);
        Assert.assertTrue("jdbc:mysql://127.0.0.1/database?useUnicode=true&characterEncoding=UTF-8&socketTimeout=3600000"
                .equals(jdbcUrl));

        jdbcUrl = AdsUtil.prepareJdbcUrl("127.0.0.1", "database", 3600000L,
                "autoReconnect=true&failOverReadOnly=false&maxReconnects=10");
        System.out.println(jdbcUrl);
        Assert.assertTrue("jdbc:mysql://127.0.0.1/database?useUnicode=true&characterEncoding=UTF-8&socketTimeout=3600000&autoReconnect=true&failOverReadOnly=false&maxReconnects=10"
                .equals(jdbcUrl));
    }

    @Test
    public void testPrepareJdbcUrl2() {
        String jdbcUrl = AdsUtil.prepareJdbcUrl(this.conf);
        System.out.println(jdbcUrl);
        Assert.assertTrue("jdbc:mysql://ads-demo-3d3dd9de.cn-hangzhou-1.ads.aliyuncs.com:3029/ads_demo?useUnicode=true&characterEncoding=UTF-8&socketTimeout=3600000"
                .equals(jdbcUrl));
    }
}
