package com.alibaba.datax.plugin.rdbms.reader;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Created by liqiang on 15/7/20.
 */
public class CommonRdbmsReaderTest {

    @Test//(expected = SQLRecoverableException.class) //Caused by: oracle.net.ns.NetException: Socket read timed out
    public void testOracleReadSocketTimeout() throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@10.232.31.103:1521:newcrm";
        String user = "tbods";
        String pass = "dwtbods";
        String tableName = "TBODS.BVT_CASE";

        //设置socket Timeout为6s
        Connection conn = DBUtil.getConnection(DataBaseType.Oracle, jdbcUrl, user, pass, "6000");


        //查询耗时10秒钟
        int runTime = 10;
        long startTime = System.nanoTime();
        //设置queryTimeout为20s
        try {
            ResultSet rs = DBUtil.query(conn, "select TBODS.SLEEP(" + runTime + ") from dual", 1000, 20);
        } catch (SQLRecoverableException e) {
            Assert.assertTrue(e.getMessage().contains("Socket read timed out"));
            return;
        }

        throw new RuntimeException("test error!");
    }

    @Test
    public void testOracleReadQueryimeout() throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@10.232.31.103:1521:newcrm";
        String user = "tbods";
        String pass = "dwtbods";
        String tableName = "TBODS.BVT_CASE";

        //设置socket Timeout为20s
        Connection conn = DBUtil.getConnection(DataBaseType.Oracle, jdbcUrl, user, pass, "20000");

        //查询耗时10秒钟
        int runTime = 10;
        long startTime = System.nanoTime();
        //设置queryTimeout为4s
        //oracle的query Timeout并没有用处，但是不影响运行结果
        ResultSet rs = DBUtil.query(conn, "select TBODS.SLEEP(" + runTime + ") from dual", 1000, 4);
        long endTime = System.nanoTime();

        System.out.println("slaps time=" + (endTime - startTime));
        //查询是10s，但是orace的sleep函数没有那么准，因此设置为8s
        Assert.assertTrue((endTime - startTime) > 8000000000L);

        startTime = System.nanoTime();
        while (DBUtil.asyncResultSetNext(rs)) {
            endTime = System.nanoTime();
            for (int i = 1; i < 2; i++) {
                System.out.println(i + "==>" + rs.getString(i) + ": slapls=" + (endTime - startTime));
            }
        }

    }

    @Test
    public void testOracleReadNormal() throws Exception {
        String jdbcUrl = "jdbc:oracle:thin:@10.232.31.103:1521:newcrm";
        String user = "tbods";
        String pass = "dwtbods";
        String tableName = "TBODS.BVT_CASE";

        //设置socket Timeout为20s
        Connection conn = DBUtil.getConnection(DataBaseType.Oracle, jdbcUrl, user, pass, "20000");

        //查询耗时10秒钟
        int runTime = 10;
        long startTime = System.nanoTime();
        //设置queryTimeout为30s
        ResultSet rs = DBUtil.query(conn, "select TBODS.SLEEP(" + runTime + ") from dual", 1000, 30);
        long endTime = System.nanoTime();

        System.out.println("slaps time=" + (endTime - startTime));
        //查询是10s，但是orace的sleep函数没有那么准，因此设置为8s
        Assert.assertTrue((endTime - startTime) > 8000000000L);

        startTime = System.nanoTime();
        while (DBUtil.asyncResultSetNext(rs)) {
            endTime = System.nanoTime();
            for (int i = 1; i < 2; i++) {
                System.out.println(i + "==>" + rs.getString(i) + ": slapls=" + (endTime - startTime));
            }
        }

    }

    @Test
    public void testMysqlReadSocketTimeout() throws Exception {
        //设置socket Timeout为2 s
        String jdbcUrl = "jdbc:mysql://10.101.83.3:3306/datax_3_mysqlreader?socketTimeout=2000";
        String user = "root";
        String pass = "root";

        Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl,
                user, pass);

        //查询10秒钟
        int runTime = 10;
        long startTime = System.nanoTime();

        //设置queryTimeout为20s
        try {
            ResultSet rs = DBUtil.query(conn, "select now(),sleep(" + runTime + ")", 1000, 20);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("Communications link failure"));
            return;
        }

        throw new RuntimeException("test run failed!");

    }

    @Test(expected = MySQLTimeoutException.class)
    public void testMysqlReadQueryTimeout() throws Exception {
        String jdbcUrl = "jdbc:mysql://10.101.83.3:3306/datax_3_mysqlreader";
        String user = "root";
        String pass = "root";

        Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl,
                user, pass);

        //查询4秒钟
        int runTime = 4;
        long startTime = System.nanoTime();

        //设置queryTimeout为2s
        ResultSet rs = DBUtil.query(conn, "select now(),sleep(" + runTime + ")", 1000, 2);
        long endTime = System.nanoTime();
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue((endTime - startTime) > 4000000000L);

        System.out.println("slaps time=" + (endTime - startTime));

        startTime = System.nanoTime();
        while (DBUtil.asyncResultSetNext(rs)) {
            endTime = System.nanoTime();
            for (int i = 1; i <= 2; i++) {
                System.out.println(i + "==>" + rs.getString(i) + ": slapls=" + (endTime - startTime));
            }
        }
    }

    @Test
    public void testMysqlQueryNormal() throws Exception {
        //设置socket timeout 为30s
        String jdbcUrl = "jdbc:mysql://10.101.83.3:3306/datax_3_mysqlreader?socketTimeout=30000";
        String user = "root";
        String pass = "root";

        Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl,
                user, pass);

        //查询10秒钟
        int runTime = 10;
        long startTime = System.nanoTime();

        //设置queryTimeout为20s
        ResultSet rs = DBUtil.query(conn, "select now(),sleep(" + runTime + ")", 1000, 20);
        long endTime = System.nanoTime();
        ResultSetMetaData metaData = rs.getMetaData();
        Assert.assertTrue((endTime - startTime) > 10000000000L);

        System.out.println("slaps time=" + (endTime - startTime));

        startTime = System.nanoTime();
        while (DBUtil.asyncResultSetNext(rs)) {
            endTime = System.nanoTime();
            for (int i = 1; i <= 2; i++) {
                System.out.println(i + "==>" + rs.getString(i) + ": slapls=" + (endTime - startTime));
            }
        }
    }

    @Test
    public void testMysqlReadResultSetTimeout() throws Exception {
        //设置socket timeout 为30s
        String jdbcUrl = "jdbc:mysql://10.101.83.3:3306/datax_3_mysqlreader?socketTimeout=30000";
        String user = "root";
        String pass = "root";

        Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl,
                user, pass);

        //查询10秒钟
        int runTime = 10;
        long startTime = System.nanoTime();

        //设置queryTimeout为20s
        final ResultSet rs = DBUtil.query(conn, "select now(),sleep(" + runTime + ")", 1000, 20);
        long endTime = System.nanoTime();
        Assert.assertTrue((endTime - startTime) > 10000000000L);

        System.out.println("slaps time=" + (endTime - startTime));

        startTime = System.nanoTime();

        //mock ResultSet
        ResultSet spy = spy(rs);

        //stub: 调用ResultSet的next时，睡眠10s，等待超时
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                sleep(10);
                return rs.next();
            }
        }).when(spy).next();

        //设置resultSet的timeout为5 sec
        try {
            while (DBUtil.asyncResultSetNext(spy, 5)) {
                endTime = System.nanoTime();
                for (int i = 1; i <= 2; i++) {
                    System.out.println(i + "==>" + rs.getString(i) + ": slapls=" + (endTime - startTime));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("异步获取ResultSet next失败."));
            return;
        }
        throw new RuntimeException("test failed!");
    }

    @Test
    public void testMysqlReadResultSetNormal() throws Exception {
        //设置socket timeout 为30s
        String jdbcUrl = "jdbc:mysql://10.101.83.3:3306/datax_3_mysqlreader?socketTimeout=30000";
        String user = "root";
        String pass = "root";

        Connection conn = DBUtil.getConnection(DataBaseType.MySql, jdbcUrl,
                user, pass);

        //查询10秒钟
        int runTime = 10;
        long startTime = System.nanoTime();

        //设置queryTimeout为20s
        final ResultSet rs = DBUtil.query(conn, "select now(),sleep(" + runTime + ")", 1000, 20);
        long endTime = System.nanoTime();
        Assert.assertTrue((endTime - startTime) > 10000000000L);

        System.out.println("slaps time=" + (endTime - startTime));

        startTime = System.nanoTime();

        //mock ResultSet
        ResultSet spy = spy(rs);

        //stub: 调用ResultSet的next时，睡眠10s，等待超时
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                sleep(10);
                return rs.next();
            }
        }).when(spy).next();

        //设置resultSet的timeout为30 sec
        while (DBUtil.asyncResultSetNext(spy, 30)) {
            endTime = System.nanoTime();
            for (int i = 1; i <= 2; i++) {
                System.out.println(i + "==>" + rs.getString(i) + ": slapls=" + (endTime - startTime));
            }
        }

    }


    @Test
    public void testMockHang() throws Exception {
        List<String> list = new ArrayList<String>();
        list.add("qs");
        List<String> spy = spy(list);

        doReturn("foo").when(spy).get(1);

        long startTime = System.nanoTime();
        String re1 = spy.get(1);

        long endtime = System.nanoTime();
        System.out.println(re1 + " => slaps: " + (endtime - startTime));

        //doReturn(sleep(10)).when(spy).get(1);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                List mock = (List) invocation.getMock();
                System.out.println("dddddd=>" + mock.toString());
                return sleep(10);
            }
        }).when(spy).get(2);

        startTime = System.nanoTime();
        String re2 = spy.get(2);

        endtime = System.nanoTime();
        System.out.println(re2 + " => slaps: " + (endtime - startTime));

    }

    public String sleep(int n) {
        System.out.println("ddddddjdjdjdjdjjdjdjdjjdjd1:" + System.currentTimeMillis());
        try {
            Thread.sleep(n * 1000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("ddddddjdjdjdjdjjdjdjdjjdjd2:" + System.currentTimeMillis());
        return "sleep end foo";
    }
}
