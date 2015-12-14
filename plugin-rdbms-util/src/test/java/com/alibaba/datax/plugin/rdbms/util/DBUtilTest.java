package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import com.alibaba.druid.sql.parser.ParserException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
public class DBUtilTest {

    //todo 跑不过的单测，先简单注释掉。原因 库地址发生变化
    @Ignore
    public void testGetTableColumns() {
        String jdbcUrl = "jdbc:mysql://10.232.130.106:3306/datax_3_mysqlreader";
        String user = "root";
        String pass = "root";
        String tableName = "`bvt_case_1rows_5split`";
        List<String> allColumns = DBUtil.getTableColumns(DataBaseType.MySql, jdbcUrl, user,
                pass, tableName);
        System.out.println(allColumns);
    }

    //todo 跑不过的单测，先简单注释掉，库地址发生变化
    @Ignore
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

    //todo 跑不过的单测，先简单注释掉
    @Ignore
    public void queryTest(){
        String jdbcUrl = "jdbc:mysql://10.232.130.106:3306/datax_3_mysqlreader";
        String user = "root";
        String pass = "root";
        String tableName = "`bvt_case_1rows_5split`";
        Connection conn = DBUtil.getConnection(DataBaseType.MySql,jdbcUrl,user,pass);

    }

    @Test
    public void sqlValidFalseTest(){
        String sql = "select distinct desc from bvt_case_1_rows_5split";
        try {
            DBUtil.sqlValid(sql,DataBaseType.MySql);
        }catch (ParserException e){
            Assert.assertNotNull(e);
        }
    }

    @Test
    public void sqlValidTrueTest(){
        String sql = "select distinct id from bvt_case_1_rows_5split";
        try {
            DBUtil.sqlValid(sql,DataBaseType.MySql);
            Assert.assertTrue(true);
        }catch (ParserException e){
            Assert.assertNull(e);
        }
    }

    @Test
    public void sqlValidNullTest(){
        String sql = "";
        try {
            DBUtil.sqlValid(sql,DataBaseType.MySql);
            Assert.assertTrue(true);
        }catch (ParserException e){
            Assert.assertNull(e);
        }
    }

    @Test
    public void sqlValidFailedTest(){
        String sql = "select ＊ from bvt_case_1_rows_5split";
        try {
            DBUtil.sqlValid(sql,DataBaseType.MySql);
        }catch (ParserException e){
            Assert.assertNull(e);
        }
    }

//    @Test(enabled=false)
//    public void mySQLDBNameErrTest(){
//        DataBaseType dataBaseType = DataBaseType.MySql;
//        List<String> jdbcUrls = new ArrayList<String>();
//        jdbcUrls.add("jdbc:mysql://10.101.83.3:3306/bad_database");
//        String username = "root";
//        String password = "root";
//        List<String> preSql = null;
//        boolean checkSlave = false;
//        try{
//            DBUtil.chooseJdbcUrl(dataBaseType,jdbcUrls,username,password,preSql,checkSlave);
//        }catch (Exception e){
//            Assert.assertEquals(e.getMessage(), DBUtilErrorCode.MYSQL_CONN_DB_ERROR.toString());
//        }
//    }
}
