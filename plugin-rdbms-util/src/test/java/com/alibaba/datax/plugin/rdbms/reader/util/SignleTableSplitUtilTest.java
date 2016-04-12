package com.alibaba.datax.plugin.rdbms.reader.util;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

public class SignleTableSplitUtilTest {

    @Test
    public void testSignleTableSplie() {
        SingleTableSplitUtil.DATABASE_TYPE = DataBaseType.Oracle;
        Configuration conf = Configuration
                .from("{'username':'yixiao','password':'yixiao_cdp','where':'id < 25000000 and id >= 0','percentage':'0.05','splitPk':'id','splitMode':'randomSampling','column':['id','name'],'jdbcUrl':'jdbc:oracle:thin:@10.101.84.197:1521:uprr'}");
        List<String> result = SingleTableSplitUtil.genSplitSqlForOracle("id",
                "reader", "", conf, 10);
        System.out.println(result);
        Assert.assertTrue(result.size() == 10);

        result = SingleTableSplitUtil.genSplitSqlForOracle("id", "reader", "",
                conf, 1);
        System.out.println(result);
        Assert.assertTrue(null == result);

        try {
            result = SingleTableSplitUtil.genSplitSqlForOracle("id", "reader",
                    "", conf, 0);
            System.out.println(result);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }
}
