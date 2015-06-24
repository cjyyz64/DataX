package com.alibaba.datax.plugin.writer.mysqlrulewriter.test;

import com.alibaba.datax.plugin.writer.mysqlrulewriter.groovy.GroovyRuleExecutor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 * Date: 15/5/8 下午3:01
 *
 * @author liupeng <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class GroovyRuleExecutorTest {

    @Test
    public void testExecuteGroovy() throws Exception {
        Map<String, Object> columnValues = new HashMap<String, Object>();
        columnValues.put("id", 15L);
        GroovyRuleExecutor groovyRule = new GroovyRuleExecutor("((#id#).longValue() % 40)", "test_{0000}");
        String result = groovyRule.executeRule(columnValues);
        assertEquals(result, "test_0015");
        System.out.println(result);
    }

    @Test
    public void testExecuteStringGroovy() throws Exception {
        Map<String, Object> columnValues = new HashMap<String, Object>();
        columnValues.put("id", "test123234234234234234234234234");
        GroovyRuleExecutor groovyRule = new GroovyRuleExecutor("String table_index = #id#.substring(13,15); int temp = Integer.parseInt(table_index); return String.format(\"%d\",(Integer)temp % 100);", "test_{0}");
        String result = groovyRule.executeRule(columnValues);
        System.out.println(groovyRule.executeRule(columnValues));
    }

    @Test
    public void testRuleExecute() {
        Map<String, Object> columnValues = new HashMap<String, Object>();
        GroovyRuleExecutor groovyRule = new GroovyRuleExecutor("((#id#).longValue() % 8).intdiv(4)", "datax_3_mysqlrulewriter_{00}");
        Long before = System.currentTimeMillis();
        for (int i = 0; i < 16; i++) {
            columnValues.put("id", i);
            System.out.println(i + " ," +  groovyRule.executeRule(columnValues));
        }
        System.out.println("耗时：" + (System.currentTimeMillis() - before));
    }

    @Test
    public void testNullExecuteGroovy() throws Exception {
        Map<String, Object> columnValues = new HashMap<String, Object>();
        columnValues.put("id", 7L);
        GroovyRuleExecutor groovyRule = new GroovyRuleExecutor("", "test11");
        System.out.println(groovyRule.executeRule(columnValues));
    }

    @Test
    public void testGetDbName() throws Exception {
        String jdbcUrl = "jdbc:mysql://10.232.130.106:3306/datax_3_mysqlwriter?yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
        System.out.println(jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1, jdbcUrl.indexOf("?")));
    }

    @Test
    public void testDbTBRules() {
        Map<String, Object> columnValues = new HashMap<String, Object>();
        columnValues.put("principal_id", "2088000012345678");
        GroovyRuleExecutor dbRule = new GroovyRuleExecutor("Long.valueOf(#principal_id#.substring(13, 15)).intdiv(5)", "proprod{00}");
        String dbResult = dbRule.executeRule(columnValues);

        GroovyRuleExecutor tbRule = new GroovyRuleExecutor("#principal_id#.substring(13, 15)", "prod_lifetime_new{00}");
        String tbResult = tbRule.executeRule(columnValues);

        System.out.println("principal_id=2088000012345678 , 将会落在" + dbResult + "分库上," + tbResult + "分表上");
    }


}
