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

}
