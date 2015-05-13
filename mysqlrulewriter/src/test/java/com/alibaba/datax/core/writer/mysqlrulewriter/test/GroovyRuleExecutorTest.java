package com.alibaba.datax.core.writer.mysqlrulewriter.test;

import com.alibaba.datax.core.writer.mysqlrulewriter.groovy.GroovyRuleExecutor;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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
        GroovyRuleExecutor groovyRule = new GroovyRuleExecutor("((#id#).longValue() % 40)", "test_{0}");
        Long before = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            System.out.println(groovyRule.eval(columnValues, null));
        }
        System.out.println("耗时：" + (System.currentTimeMillis() - before));
    }

    @Test
    public void testRuleExecute() {
        Map<String, Object> columnValues = new HashMap<String, Object>();
        GroovyRuleExecutor groovyRule = new GroovyRuleExecutor("((#id#).longValue() % 8).intdiv(4)", "datax_3_mysqlrulewriter_{00}");
        Long before = System.currentTimeMillis();
        for (int i = 0; i < 16; i++) {
            columnValues.put("id", i);
            System.out.println(i + " ," +  groovyRule.eval(columnValues, null));
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
}
