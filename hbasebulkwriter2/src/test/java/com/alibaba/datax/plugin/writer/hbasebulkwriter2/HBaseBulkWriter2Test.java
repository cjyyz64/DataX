package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf.DynamicColumnConf;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf.FixColumnConf;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf.HBaseJobParameterConf;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liqiang on 15/5/25.
 */
public class HBaseBulkWriter2Test {

    @Test
    public void testGetPartitions() throws Exception {
        ArrayList<String> lists = Lists.newArrayList("dt=123", "dt=234", "dt=345", "");

        ArrayList<String> resList = Lists.newArrayList(lists);
        for (String v : lists) {
            if (Strings.isNullOrEmpty(v)) {
                resList.remove(v);
            }
        }

        String expect1 = Joiner.on("/").join(resList);

        HBaseBulkWriter2.Job job = new HBaseBulkWriter2.Job();
        Method initMethod = job.getClass()
                .getDeclaredMethod("getPartitions", List.class);
        initMethod.setAccessible(true);
        String res = (String) initMethod.invoke(job, lists);

        Assert.assertEquals(expect1, "dt=123/dt=234/dt=345");
        Assert.assertEquals(res, "dt=123/dt=234/dt=345");

    }

    @Test
    public void testGetSortFixColumn() throws Exception {
        ArrayList<String> odps_column = Lists.newArrayList("id", "name", "age", "birthday", "sedn");
        String hbase_rowkey = "0|string,3|int";

        HBaseBulkWriter2.Job job = new HBaseBulkWriter2.Job();
        Method method = job.getClass()
                .getDeclaredMethod("getSortColumn", List.class, String.class, String.class);
        method.setAccessible(true);
        String res1 = (String) method.invoke(job, odps_column, hbase_rowkey, null);

        System.out.println(res1);
        Assert.assertEquals(res1, "string:id,int:birthday");


        String hbase_rowkey2 = "0|string,-1|string|test1000";
        String res2 = (String) method.invoke(job, odps_column, hbase_rowkey2, null);
        System.out.println(res2);
        Assert.assertEquals(res2, "string:id,string:'test1000'");
    }

    @Test
    public void testDynamicColumn() throws Exception {
        ArrayList<String> odps_column = Lists.newArrayList("rowkey", "cf_qual", "ts", "val");
        String rowkey_type = "string";

        HBaseBulkWriter2.Job job = new HBaseBulkWriter2.Job();
        Method method = job.getClass()
                .getDeclaredMethod("getSortColumn", List.class, String.class, String.class);
        method.setAccessible(true);
        String res1 = (String) method.invoke(job, odps_column, null, rowkey_type);

        System.out.println(res1);
        Assert.assertEquals(res1, "string:rowkey,string:cf_qual,bigint:ts");
    }

    @Test(expected = InvocationTargetException.class)
    public void testGetSortFixColumnException() throws Exception {
        ArrayList<String> odps_column = Lists.newArrayList("id", "name", "age", "birthday", "sedn");

        //设置错误id：5
        String hbase_rowkey = "0|string,5|int";

        HBaseBulkWriter2.Job job = new HBaseBulkWriter2.Job();
        Method method = job.getClass()
                .getDeclaredMethod("getSortColumn", List.class, String.class, String.class);
        method.setAccessible(true);
        String res1 = (String) method.invoke(job, odps_column, hbase_rowkey, null);

    }

    @Test(expected = InvocationTargetException.class)
    public void testGetSortFixColumnException2() throws Exception {
        ArrayList<String> odps_column = Lists.newArrayList("id", "name", "age", "birthday", "sedn");

        //设置错误id：5
        String hbase_rowkey = "0|string,3|int";

        HBaseBulkWriter2.Job job = new HBaseBulkWriter2.Job();
        Method method = job.getClass()
                .getDeclaredMethod("getSortColumn", List.class, String.class, String.class);
        method.setAccessible(true);
        String res1 = (String) method.invoke(job, odps_column, hbase_rowkey, null);

        System.out.println(res1);
        Assert.assertEquals(res1, "string:id,int:birthday");


        String hbase_rowkey2 = "0|string,-1|string";
        String res2 = (String) method.invoke(job, odps_column, hbase_rowkey2, null);
        System.out.println(res2);
        Assert.assertEquals(res2, "string:id,string:'test1000'");
    }

    @Test(expected = InvocationTargetException.class)
    public void testDynamicColumnException() throws Exception {
        ArrayList<String> odps_column = Lists.newArrayList("rowkey", "cf_qual", "ts", "val", "");
        String rowkey_type = "string";

        HBaseBulkWriter2.Job job = new HBaseBulkWriter2.Job();
        Method method = job.getClass()
                .getDeclaredMethod("getSortColumn", List.class, String.class, String.class);
        method.setAccessible(true);
        String res1 = (String) method.invoke(job, odps_column, null, rowkey_type);

        System.out.println(res1);
        Assert.assertEquals(res1, "string:rowkey,string:cf_qual,bigint:ts");
    }


    @Test
    public void testWriterJobJsonForFix() throws Exception {
        String path = HBaseBulkWriter2Test.class.getClassLoader()
                .getResource("fixcolumn_job0.json").getFile();
        Configuration configuration = Configuration.from(new File(path));


        Configuration readerOriginPluginConf = configuration.getConfiguration(Key.READER_PARAMETER);
        Configuration writerOriginPluginConf = configuration.getConfiguration(Key.WRITER_PARAMETER + "." + Key.PARAMETER_TYPE_ORIGIN);
        //"job.content[0].reader.parameter.column"
        List<String> odps_column = readerOriginPluginConf.getList(Key.KEY_COLUMN, String.class);
        String hbase_rowkey = writerOriginPluginConf.getString(Key.KEY_HBASE_ROWKEY);
        String rowkey_type = writerOriginPluginConf.getString(Key.KEY_ROWKEY_TYPE);




        HBaseBulkWriter2.Job job = new HBaseBulkWriter2.Job();
        Method method = job.getClass()
                .getDeclaredMethod("getSortColumn", List.class,String.class,String.class);
        method.setAccessible(true);
        String sort_column= (String)method.invoke(job, odps_column, hbase_rowkey, rowkey_type);

        System.out.println(sort_column);
        System.out.println(JSON.toJSONString(job.fixColumnConf));

        //change the origin configuration

        //for reader:
        configuration.set("job.content[0].reader.parameter.table", "t_datax_odps2hbase_table");
        configuration.set("job.content[0].reader.parameter.partition", "datax_pt=*");

        Method method2 = job.getClass()
                .getDeclaredMethod("getFixColumnConf",Configuration.class);
        method2.setAccessible(true);
        HBaseJobParameterConf res= (HBaseJobParameterConf)method2.invoke(job, writerOriginPluginConf);
        Assert.assertTrue(res instanceof FixColumnConf);
        System.out.println(JSON.toJSONString(res));

        Assert.assertEquals(JSON.toJSONString(res),"{\"hbase_column\":[{\"hname\":\"cf:name\",\"htype\":\"string\",\"index\":\"1\"},{\"hname\":\"cf:age\",\"htype\":\"int\",\"index\":\"2\"},{\"hname\":\"cf:birthday\",\"htype\":\"string\",\"index\":\"3\"}],\"hbase_config\":\"test_hbase_config\",\"hbase_output\":\"test_hbase_output\",\"hbase_rowkey\":[{\"htype\":\"string\",\"index\":\"0\"}],\"hbase_table\":\"test_hbase_table\",\"hdfs_config\":\"test_hdfs_config\"}");

        configuration.set("job.content[0].writer.parameter.fixedcolumn", JSON.toJSONString(res));

        System.out.println(configuration.toString());
    }

    @Test
    public void testWriterJobJsonForDynamic() throws Exception {
        String path = HBaseBulkWriter2Test.class.getClassLoader()
                .getResource("dynamic_job0.json").getFile();
        Configuration configuration = Configuration.from(new File(path));


        Configuration readerOriginPluginConf = configuration.getConfiguration(Key.READER_PARAMETER);
        Configuration writerOriginPluginConf = configuration.getConfiguration(Key.WRITER_PARAMETER + "." + Key.PARAMETER_TYPE_ORIGIN);
        //"job.content[0].reader.parameter.column"
        List<String> odps_column = readerOriginPluginConf.getList(Key.KEY_COLUMN, String.class);
        String hbase_rowkey = writerOriginPluginConf.getString(Key.KEY_HBASE_ROWKEY);
        String rowkey_type = writerOriginPluginConf.getString(Key.KEY_ROWKEY_TYPE);




        HBaseBulkWriter2.Job job = new HBaseBulkWriter2.Job();
        Method method = job.getClass()
                .getDeclaredMethod("getSortColumn", List.class,String.class,String.class);
        method.setAccessible(true);
        String sort_column= (String)method.invoke(job, odps_column, hbase_rowkey, rowkey_type);

        System.out.println(sort_column);
        System.out.println(JSON.toJSONString(job.fixColumnConf));

        //change the origin configuration

        //for reader:
        configuration.set("job.content[0].reader.parameter.table", "t_datax_odps2hbase_table");
        configuration.set("job.content[0].reader.parameter.partition", "datax_pt=*");

        Method method2 = job.getClass()
                .getDeclaredMethod("getDynamicColumnConf",Configuration.class);
        method2.setAccessible(true);
        HBaseJobParameterConf res= (HBaseJobParameterConf)method2.invoke(job, writerOriginPluginConf);
        Assert.assertTrue(res instanceof DynamicColumnConf);
        System.out.println(JSON.toJSONString(res));

//        Assert.assertEquals(JSON.toJSONString(res),"{\"hbase_column\":[{\"hname\":\"cf:name\",\"htype\":\"string\",\"index\":\"1\"},{\"hname\":\"cf:age\",\"htype\":\"int\",\"index\":\"2\"},{\"hname\":\"cf:birthday\",\"htype\":\"string\",\"index\":\"3\"}],\"hbase_config\":\"test_hbase_config\",\"hbase_output\":\"test_hbase_output\",\"hbase_rowkey\":[{\"htype\":\"string\",\"index\":\"0\"}],\"hbase_table\":\"test_hbase_table\",\"hdfs_config\":\"test_hdfs_config\"}");

        configuration.set("job.content[0].writer.parameter.fixedcolumn", JSON.toJSONString(res));

        System.out.println(configuration.toString());
    }

}
