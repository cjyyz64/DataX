package com.alibaba.datax.plugin.writer.mysqlrulewriter.test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.writer.mysqlrulewriter.MysqlRuleCommonRdbmsWriter;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;

import static junit.framework.Assert.assertEquals;

/**
 * Date: 2015/5/19 16:33
 *
 * @author Administrator <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class MysqlRuleCommonRdbmsWriterTest {

    @Test
    public void testTaskInit() throws Exception {
        MysqlRuleCommonRdbmsWriter.Task task = new MysqlRuleCommonRdbmsWriter.Task(DataBaseType.MySql);
        Configuration jobConf = ConfigParser.parseJobConfig(Thread.currentThread().getClass().getResource("/").getPath() + File.separator + "basic1.json");
        jobConf = jobConf.getConfiguration(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);
        jobConf.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, "");
        task.init(jobConf);

        assertEquals((String) ReflectionTestUtils.getField(task, "dbNamePattern"), "datax_3_mysqlrulewriter_{00}");
        assertEquals((String) ReflectionTestUtils.getField(task, "dbRule"), "((#i1#).longValue() % 8).intdiv(4)");
        assertEquals((String) ReflectionTestUtils.getField(task, "tableNamePattern"), "mysql_writer_test_case00_{0}");
        assertEquals((String) ReflectionTestUtils.getField(task, "tableRule"), "((#i1#).longValue() % 8)");
    }

    @Test
    public void testGetDbNameFromJdbcUrl() {
        MysqlRuleCommonRdbmsWriter.Task task = new MysqlRuleCommonRdbmsWriter.Task(DataBaseType.MySql);
        String jdbcUrl = "jdbc:mysql://10.232.130.106:3306/datax_3_mysqlwriter?yearIsDateType=false&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";
        assertEquals("datax_3_mysqlwriter", task.getDbNameFromJdbcUrl(jdbcUrl));

        jdbcUrl = "jdbc:mysql://10.232.130.106:3306/datax_3_mysqlwriter";
        assertEquals("datax_3_mysqlwriter", task.getDbNameFromJdbcUrl(jdbcUrl));
    }

}
