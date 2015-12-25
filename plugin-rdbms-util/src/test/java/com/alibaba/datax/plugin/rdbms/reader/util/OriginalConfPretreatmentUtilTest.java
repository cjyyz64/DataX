package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import junit.framework.Assert;
import org.junit.Ignore;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Created by liqiang on 15/10/12.
 */
public class OriginalConfPretreatmentUtilTest {
    //todo 单元测试不过，因为oracle库发生了变化
    @Ignore
    public void testName() throws Exception {

        //没有presql，不进行检查
        String config = "{\n" +
                "  \"username\": \"tbods\",\n" +
                "  \"password\": \"dwtbods\",\n" +
                "  \"isTableMode\": true,\n" +
                "  \"column\": [\n" +
                "    \"*\"\n" +
                "  ],\n" +
                "  \"splitPk\": \"id\",\n" +
                "  \"hint\": \"/*+INDEX(student INDEX_NAME)*/\",\n" +
                "  \"where\": \"(id = 0 or id = 1) and  col1 != 0\",\n" +
                "  \"mandataryEncoding\": \"gbk\",\n" +
                "  \"session\": [\n" +
                "    \"ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI'\",\n" +
                "    \"ALTER SESSION SET NLS_LANGUAGE = 'american'\"\n" +
                "  ],\n" +
                "  \"connection\": [\n" +
                "    {\n" +
                "      \"table\": [\n" +
                "        \"TBODS.BVT_CASE\"\n" +
                "      ],\n" +
                "      \"jdbcUrl\": [\n" +
                "        \"jdbc:oracle:thin:@10.232.31.103:1521:newcrm\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        Configuration configuration = Configuration.from(config);

        Method dealJdbcAndTable = OriginalConfPretreatmentUtil.class.getDeclaredMethod("dealJdbcAndTable", Configuration.class);
        dealJdbcAndTable.setAccessible(true);

        Field f = OriginalConfPretreatmentUtil.class.getDeclaredField("DATABASE_TYPE");
        f.setAccessible(true);
        f.set(OriginalConfPretreatmentUtil.class, DataBaseType.Oracle);

        dealJdbcAndTable.invoke(OriginalConfPretreatmentUtil.class, configuration);

        int res = configuration.getInt("tableNumber");

        Assert.assertEquals(res, 1);

        // 增加preSql, preSql返回1（备库检查不通过）
        String config_slave_fail = "{\n" +
                "  \"username\": \"tbods\",\n" +
                "  \"password\": \"dwtbods\",\n" +
                "  \"isTableMode\": true,\n" +
                "  \"column\": [\n" +
                "    \"*\"\n" +
                "  ],\n" +
                "  \"splitPk\": \"id\",\n" +
                "  \"hint\": \"/*+INDEX(student INDEX_NAME)*/\",\n" +
                "  \"where\": \"(id = 0 or id = 1) and  col1 != 0\",\n" +
                "  \"mandataryEncoding\": \"gbk\",\n" +
                "  \"session\": [\n" +
                "    \"ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI'\",\n" +
                "    \"ALTER SESSION SET NLS_LANGUAGE = 'american'\"\n" +
                "  ],\n" +
                "  \"connection\": [\n" +
                "    {\n" +
                "      \"table\": [\n" +
                "        \"TBODS.BVT_CASE\"\n" +
                "      ],\n" +
                "      \"jdbcUrl\": [\n" +
                "        \"jdbc:oracle:thin:@10.232.31.103:1521:newcrm\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"preSql\":[\n" +
                "    \"select 1 from dual\"\n" +
                "    ]\n" +
                "}";


        Configuration configurationSlaveFail = Configuration.from(config_slave_fail);
        try {
            dealJdbcAndTable.invoke(OriginalConfPretreatmentUtil.class, configurationSlaveFail);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));

            System.out.println(sw.toString());
            Assert.assertTrue(sw.toString().contains("Code:[DBUtilErrorCode-10], Description:[连接数据库失败"));
        }

        //preSql检查成功

        String config_slave_suc = "{\n" +
                "  \"username\": \"tbods\",\n" +
                "  \"password\": \"dwtbods\",\n" +
                "  \"isTableMode\": true,\n" +
                "  \"column\": [\n" +
                "    \"*\"\n" +
                "  ],\n" +
                "  \"splitPk\": \"id\",\n" +
                "  \"hint\": \"/*+INDEX(student INDEX_NAME)*/\",\n" +
                "  \"where\": \"(id = 0 or id = 1) and  col1 != 0\",\n" +
                "  \"mandataryEncoding\": \"gbk\",\n" +
                "  \"session\": [\n" +
                "    \"ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI'\",\n" +
                "    \"ALTER SESSION SET NLS_LANGUAGE = 'american'\"\n" +
                "  ],\n" +
                "  \"connection\": [\n" +
                "    {\n" +
                "      \"table\": [\n" +
                "        \"TBODS.BVT_CASE\"\n" +
                "      ],\n" +
                "      \"jdbcUrl\": [\n" +
                "        \"jdbc:oracle:thin:@10.232.31.103:1521:newcrm\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"preSql\":[\n" +
                "    \"select 0 from dual\"\n" +
                "    ]\n" +
                "}";


        Configuration configurationSlaveSuc = Configuration.from(config_slave_suc);
        dealJdbcAndTable.invoke(OriginalConfPretreatmentUtil.class, configurationSlaveSuc);
        int res1 = configuration.getInt("tableNumber");

        Assert.assertEquals(res1, 1);
    }
}