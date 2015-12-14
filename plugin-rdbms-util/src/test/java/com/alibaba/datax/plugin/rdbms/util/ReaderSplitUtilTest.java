package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;

/**
 * Date: 2015/1/30 15:52
 *
 * @author tianjin.lp <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class ReaderSplitUtilTest {

    @Before
    public void setUp() throws Exception {
        OriginalConfPretreatmentUtil.DATABASE_TYPE = DataBaseType.MySql;
        SingleTableSplitUtil.DATABASE_TYPE = DataBaseType.MySql;
    }


    //todo 跑不过的单测，先简单注释掉。原因 库地址发生变化
    @Ignore
    public void testSplit单表按主键切分自动增加切分Num() throws Exception {
        DataBaseType dataBaseType = DataBaseType.MySql;
        //总体切分数充足
        Configuration readerConfig = getAndInitConfigFromClasspath("mysqlreader_pk_enough.json",dataBaseType);
        //切分数为3
        List<Configuration> configList = ReaderSplitUtil.doSplit(readerConfig, 3);
        //单表按主键切分，切分数为n*2+1，在加上pk is null 的情况，一共为2*(n+1)份
        System.out.println(configList.size());
        Assert.assertEquals(configList.size(), 8);
    }

    @Test
    public void testPrecheckSplitPk() throws Exception {

        Connection conn = DBUtil.getConnectionWithoutRetry(DataBaseType.MySql, "jdbc:mysql://10.101.83.3:3306/datax_3_mysqlreader",
                "root", "root");
        String table = "bvt_case";
        String username = "root";
        String pkSql = "select MIN(id), MAX(id) from " + table;
        SingleTableSplitUtil.precheckSplitPk(conn, pkSql, Integer.MIN_VALUE, table, username);

        String pkSql2 = "select MIN(id,col1), MAX(id,col1) from " + table;
        try {
            SingleTableSplitUtil.precheckSplitPk(conn, pkSql2, Integer.MIN_VALUE, table, username);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("读取数据库数据失败. 请检查您的配置的"));
        }

        String pkSql3 = "select MIN(col7), MAX(col7) from " + table;
        try {
            SingleTableSplitUtil.precheckSplitPk(conn, pkSql3, Integer.MIN_VALUE, table, username);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("您填写的主键列不合法, DataX 仅支持切分主键为一个,并且类型为整数或者字符串类型"));
        }

    }

    //todo 跑不过的单测，先简单注释掉。原因 库地址发生变化
    @Ignore
    public void testSplit单表按主键切分全表总切分数小于channel数() throws Exception {
        DataBaseType dataBaseType = DataBaseType.MySql;
        //总体切分数最多是4个
        Configuration readerConfig = getAndInitConfigFromClasspath("mysqlreader_pk_not_enough.json",dataBaseType);
        //切分数为3
        List<Configuration> configList = ReaderSplitUtil.doSplit(readerConfig, 3);
        //单表按主键切分，切分数为n*2+1，在加上pk is null 的情况，一共为2*(n+1)份
        System.out.println(configList.size());
        Assert.assertEquals(configList.size(), 4);
    }

    public Configuration getAndInitConfigFromClasspath(String classpathConfigName,DataBaseType dataBaseType) throws IOException {
        String configStr = FileUtils.readFileToString(new File(Thread.currentThread().getContextClassLoader().getResource(classpathConfigName).getFile()));
        Configuration readerConfig = Configuration.from(configStr);
        readerConfig.set("fetchSize", Integer.MIN_VALUE);
        OriginalConfPretreatmentUtil.DATABASE_TYPE= dataBaseType;
        OriginalConfPretreatmentUtil.doPretreatment(readerConfig);
        return readerConfig;
    }

    @Test
    public void preCheckSplitTest(){
        Configuration originalConf;
        try{
            DataBaseType dataBaseType = DataBaseType.MySql;
            originalConf = getAndInitConfigFromClasspath("mysqlreader_multiTable.json",dataBaseType);
            Configuration queryConf = ReaderSplitUtil.doPreCheckSplit(originalConf);
            List<Object> conns = queryConf.getList(Constant.CONN_MARK, Object.class);
            for (int i = 0, len = conns.size(); i < len; i++) {
                Configuration connConf = Configuration.from(conns.get(i).toString());
                List<Object> querys = connConf.getList(Key.QUERY_SQL, Object.class);
                Assert.assertEquals(2,querys.size());
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    /*实跑表名配置错误*/
    @Test
    public void tableNameErrTest(){
        DataBaseType dataBaseType = DataBaseType.MySql;
        Configuration originalConf;
        try{
            originalConf = getAndInitConfigFromClasspath("mysqlreader_dbName_Err.json",dataBaseType);
        }catch (Exception e){
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().toString().contains("连接数据库失败"));
        }
    }

    /*PreCheck 表名配置错误*/
    @Test
    public void preCheckTableNameErrTest(){
        DataBaseType dataBaseType = DataBaseType.MySql;
        try{
            getAndInitConfigFromClasspath("mysqlreader_dbName_Err.json",dataBaseType);
        }catch (Exception e){
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().toString().contains("连接数据库失败"));
        }
    }

    /*实跑dbName配置错误*/
    @Test
    public void dbNameErrTest(){
        DataBaseType dataBaseType = DataBaseType.MySql;
        try{
            getAndInitConfigFromClasspath("mysqlreader_dbName_Err.json",dataBaseType);
        }catch (Exception e){
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().toString().contains("连接数据库失败"));
        }
    }

    /*PreCheck dbName配置错误*/
    @Test
    public void preCheckDbNameErrTest(){
        DataBaseType dataBaseType = DataBaseType.MySql;
        try{
            getAndInitConfigFromClasspath("mysqlreader_dbName_Err.json",dataBaseType);
        }catch (Exception e){
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().toString().contains("连接数据库失败"));
        }
    }

    /*实跑用户名配置错误*/
    @Test
    public void userNameErrTest(){
        DataBaseType dataBaseType = DataBaseType.MySql;
        try{
            getAndInitConfigFromClasspath("mysqlreader_userName_Err.json",dataBaseType);
        }catch (Exception e){
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().toString().contains("连接数据库失败"));
        }
    }

    /*PreCheck 用户名配置错误*/
    @Test
    public void preCheckUserNameErrTest(){
        DataBaseType dataBaseType = DataBaseType.MySql;
        try{
            getAndInitConfigFromClasspath("mysqlreader_userName_Err.json",dataBaseType);
        }catch (Exception e){
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().toString().contains("连接数据库失败"));
        }
    }


    @Test
    public void testDealWhere() {
        Configuration configuration = Configuration.newDefault();
        String whereCondition = "gmtCreate > '2015-05-07'";

        String whereTest1 = whereCondition + ";";
        configuration.set("where",whereTest1);
        OriginalConfPretreatmentUtil.dealWhere(configuration);
        String whereTest1Improve = configuration.getString("where");
        Assert.assertEquals(whereTest1Improve,whereCondition);

        String whereTest2 = whereCondition + ";  ";
        configuration.set("where",whereTest2);
        OriginalConfPretreatmentUtil.dealWhere(configuration);
        String whereTest2Improve = configuration.getString("where");
        Assert.assertEquals(whereTest2Improve,whereCondition);


        String whereTest3 = whereCondition + "；";
        configuration.set("where",whereTest3);
        OriginalConfPretreatmentUtil.dealWhere(configuration);
        String whereTest3Improve = configuration.getString("where");
        Assert.assertEquals(whereTest3Improve,whereCondition);

        String whereTest4 = whereCondition + "；   ";
        configuration.set("where",whereTest4);
        OriginalConfPretreatmentUtil.dealWhere(configuration);
        String whereTest4Improve = configuration.getString("where");
        Assert.assertEquals(whereTest4Improve,whereCondition);

        System.out.println("whereCondition: " + whereCondition);
        System.out.println("----------------------------------");
        System.out.println("whereTest1: " + whereTest1);
        System.out.println("whereTest2: " + whereTest2);
        System.out.println("whereTest3: " + whereTest3);
        System.out.println("whereTest4: " + whereTest4);
        System.out.println("----------------------------------");
        System.out.println("whereTest1Improve: " + whereTest1Improve);
        System.out.println("whereTest2Improve: " + whereTest2Improve);
        System.out.println("whereTest3Improve: " + whereTest3Improve);
        System.out.println("whereTest4Improve: " + whereTest4Improve);
    }

}
