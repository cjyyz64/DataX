package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.OriginalConfPretreatmentUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.ReaderSplitUtil;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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

    @Test
    public void testSplit单表按主键切分自动增加切分Num() throws Exception {
        //总体切分数充足
        Configuration readerConfig = getAndInitConfigFromClasspath("mysqlreader_pk_enough.json");
        //切分数为3
        List<Configuration> configList = ReaderSplitUtil.doSplit(readerConfig, 3);
        //单表按主键切分，切分数为n*2+1，在加上pk is null 的情况，一共为2*(n+1)份
        System.out.println(configList.size());
        Assert.assertEquals(configList.size(), 8);
    }

    @Test
    public void testSplit单表按主键切分全表总切分数小于channel数() throws Exception {
        //总体切分数最多是4个
        Configuration readerConfig = getAndInitConfigFromClasspath("mysqlreader_pk_not_enough.json");
        //切分数为3
        List<Configuration> configList = ReaderSplitUtil.doSplit(readerConfig, 3);
        //单表按主键切分，切分数为n*2+1，在加上pk is null 的情况，一共为2*(n+1)份
        System.out.println(configList.size());
        Assert.assertEquals(configList.size(), 4);
    }

    public Configuration getAndInitConfigFromClasspath(String classpathConfigName) throws IOException {
        String configStr = FileUtils.readFileToString(new File(Thread.currentThread().getContextClassLoader().getResource(classpathConfigName).getFile()));
        Configuration readerConfig = Configuration.from(configStr);
        readerConfig.set("fetchSize", Integer.MIN_VALUE);
        OriginalConfPretreatmentUtil.doPretreatment(readerConfig);
        return readerConfig;
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
