package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
public class DBUtilTest {

    @Test
    public void testGetTableColumns() {
        String jdbcUrl = "jdbc:mysql://10.232.130.106:3306/datax_3_mysqlreader";
        String user = "root";
        String pass = "root";
        String tableName = "`bvt_case_1rows_5split`";
        List<String> allColumns = DBUtil.getTableColumns(DataBaseType.MySql, jdbcUrl, user,
                pass, tableName);
        System.out.println(allColumns);
    }

    @Test
    public void testGetTableColumns_Oracle() {
        String jdbcUrl = "jdbc:oracle:thin:@//10.232.128.67:1521/dataplat";
        String user = "dataplat";
        String pass = "dataplat";
        String tableName = "dwa.ETL_DATAX_LOG_NEW";
        List<String> allColumns = DBUtil.getTableColumns(DataBaseType.Oracle, jdbcUrl, user,
                pass, tableName);
        System.out.println(allColumns);

    }

    @Test
    public void testRenderPreOrPostSqls() {
        Assert.assertTrue(WriterUtil.renderPreOrPostSqls(null, "tableName").isEmpty());
        List<String> sqls = new ArrayList<String>();
        sqls.clear();
        sqls.add(" select * from @table");
        sqls.add(" ");
        sqls.add(null);
        sqls.add("");
        List<String> finalSqls = WriterUtil.renderPreOrPostSqls(sqls, "biz_order_id");
        Assert.assertTrue(finalSqls.size() == 1 && " select * from biz_order_id".equals(finalSqls.get(0)));

    }
}
