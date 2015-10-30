package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsHelper;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * HdfsHelper Tester.
 *
 * @author <Authors name>
 * @since <pre>十月 17, 2015</pre>
 * @version 1.0
 */
public class HdfsHelperTest {
    HdfsHelper hdfsHelper = null;
    String defaultFS = null;

    @Before
    public void before() throws Exception {
        defaultFS ="hdfs://10.101.204.12:9000";
        hdfsHelper = new HdfsHelper();
        hdfsHelper.getFileSystem(defaultFS);
    }

    @After
    public void after() throws Exception {
        hdfsHelper.closeFileSystem();
    }

    /**
     *
     * Method: getFileSystem(String defaultFS)
     *
     */
    @Test
    public void testGetFileSystem() throws Exception {
        String defaultFS ="hdfs://10.101.204.12:9000";
        hdfsHelper.getFileSystem(defaultFS);
        Assert.assertTrue((hdfsHelper.fileSystem != null));
    }

    /**
     *
     * Method: hdfsDirList(String dir)
     *
     */
    @Test
    public void testHdfsDirListDir() throws Exception {
        String path = "/user/hive/warehouse/writer.db/text";
        String [] files = hdfsHelper.hdfsDirList(path);
        for(int i =0;i<files.length;i++){
            System.out.println(files[i]);
        }
        int size = files.length;
        Assert.assertEquals(size,1);
    }

    /**
     *
     * Method: hdfsDirList(String dir, String fileName)
     *
     */
    @Test
    public void testHdfsDirListForDirFileName() throws Exception {
        String path ="/user/hive/warehouse/writerorc.db/orc/";
        int size = hdfsHelper.hdfsDirList(path,"qiran").length;
        Assert.assertEquals(size,1);
    }

    /**
     *
     * Method: isPathexists(String filePath)
     *
     */
    @Test
    public void testIsPathexists() throws Exception {
        String path1 ="/user/hive/warehouse/writerorc.db/orc/";
        String path2 ="/user/hive/warehouse/writerorc.db/orcxxxxxx/";
        String path3= "user/hive/warehouse/hdfswriter.db/text_table";
        Boolean  is1 = hdfsHelper.isPathexists(path1);
        Boolean  is2 = hdfsHelper.isPathexists(path2);
        Boolean  is3 = hdfsHelper.isPathexists(path3);
        System.out.println(is3);
        Assert.assertEquals(is1,true);
        Assert.assertEquals(is2,false);
    }

    /**
     *
     * Method: isPathDir(String filePath)
     *
     */
    @Test
    public void testIsPathDir() throws Exception {
        String path1 ="/user/hive/warehouse/writerorc.db/orc/";
        String path2 ="/user/hive/warehouse/writerorc.db/orc/a";
        Boolean  is1 = hdfsHelper.isPathDir(path1);
        Boolean  is2 = hdfsHelper.isPathDir(path2);
        Assert.assertEquals(is1,true);
        Assert.assertEquals(is2,false);
    }

    /**
     *
     * Method: deleteFiles(Path[] paths)
     *
     */
    @Test
    public void testDeleteFiles() throws Exception {
//TODO: Test goes here...
    }

    /**
     *
     * Method: closeFileSystem()
     *
     */
    @Test
    public void testCloseFileSystem() throws Exception {
//TODO: Test goes here...
    }

    /**
     *
     * Method: getOutputStream(String path)
     *
     */
    @Test
    public void testGetOutputStream() throws Exception {
        String path = "/user/hive/warehouse/writer.db/text/a";
        FSDataOutputStream outputStream = (FSDataOutputStream)hdfsHelper.getOutputStream(path);
        Assert.assertTrue((outputStream != null));

    }

    /**
     *
     * Method: orcFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName, TaskPluginCollector taskPluginCollector)
     *
     */
    @Test
    public void testOrcFileStartWrite() throws Exception {
//TODO: Test goes here...
    }

    /**
     *
     * Method: getColumnNames(List<Configuration> columns)
     *
     */
    @Test
    public void testGetColumnNames() throws Exception {
        String path = "/Users/shf/workplace/GitWorkplace/datax/hdfswriter/src/test/java/resources/basic1.json";
        Configuration configuration = Configuration.from(new File(path));
        Configuration writerOriginPluginConf = configuration.getConfiguration("job.content[0].writer.parameter");
        List<Configuration> columns = writerOriginPluginConf.getListConfiguration("column");
        //System.out.println(JSON.toJSONString(columns));
        System.out.println(columns);
        List<String> columnNames = Lists.newArrayList();
        for(int i=1;i<14;i++){
            columnNames.add("col"+i);
        }
        List<String> columnNamesTest =hdfsHelper.getColumnNames(columns);
        System.out.println(columnNamesTest.toString());
        Assert.assertEquals(columnNamesTest,columnNames);
    }

    /**
     *
     * Method: getColumnTypeInspectors(List<Configuration> columns)
     *
     */
    @Test
    public void testGetColumnTypeInspectors() throws Exception {
        String path = "/Users/shf/workplace/GitWorkplace/datax/hdfswriter/src/test/java/resources/basic1.json";
        Configuration configuration = Configuration.from(new File(path));
        Configuration writerOriginPluginConf = configuration.getConfiguration("job.content[0].writer.parameter");
        List<Configuration> columns = writerOriginPluginConf.getListConfiguration("column");
        List<ObjectInspector> columnTypeInspectorsTest = hdfsHelper.getColumnTypeInspectors(columns);

        List<ObjectInspector> columnTypeInspectors = Lists.newArrayList();
        /**
         TINYINT,
         SMALLINT,
         INT,
         BIGINT,
         FLOAT,
         DOUBLE,
         DECIMAL,
         TIMESTAMP,
         DATE,
         STRING,
         VARCHAR,
         CHAR,
         BOOLEAN
         */

        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(BigDecimal.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        columnTypeInspectors.add(ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        Assert.assertEquals(columnTypeInspectorsTest, columnTypeInspectors);
    }

    /**
     *
     * Method: getOrcSerde(Configuration config)
     *
     */
    @Test
    public void testGetOrcSerde() throws Exception {
        String path = "/Users/shf/workplace/GitWorkplace/datax/hdfswriter/src/test/java/resources/basic1.json";
        Configuration configuration = Configuration.from(new File(path));
        Configuration writerOriginPluginConf = configuration.getConfiguration("job.content[0].writer.parameter");

        OrcSerde orcSerde = hdfsHelper.getOrcSerde(writerOriginPluginConf);
        Assert.assertTrue((orcSerde != null));

    }

    /**
     *
     * Method: transportOneRecord(Record record, String nullFormat, List<Configuration> columnsConfiguration, TaskPluginCollector taskPluginCollector)
     *
     */
    @Test
    public void testTransportOneRecord() throws Exception {

        String time ="2015-10-18 17:40:20";
//        System.out.println(new Timestamp(System.currentTimeMillis()));
//        System.out.println(new Date(System.currentTimeMillis()));
//        SimpleDateFormat DateFormate =   new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//        java.util.Date utilDate = DateFormate.parse(time);
//
//        java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
//        java.sql.Timestamp tm = new java.sql.Timestamp(utilDate.getTime());
//        System.out.println(utilDate);
//        System.out.println(sqlDate.toString());
//        System.out.println(tm);
//        System.out.println(java.sql.Date.valueOf(DateFormate.format(sqlDate)));

        SimpleDateFormat DateFormate =   new SimpleDateFormat("yyyy.MM.dd");
        java.util.Date utilDate = new java.util.Date();
        java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
        System.out.println("utildate:" + utilDate);
        System.out.println("sqldate" + sqlDate);
        System.out.println("格式化");
        String formateUtilDate = DateFormate.format(utilDate);
        String formateSqlDate = DateFormate.format(utilDate);
        System.out.println(formateUtilDate);
        System.out.println(formateSqlDate);
        //java.util.Date formateutilDate = new java.util.Date(formateUtilDate);
        java.sql.Date formatesqlDate  = new Date(DateFormate.parse(formateSqlDate).getTime()) ;
        //System.out.println(formateutilDate);
        System.out.println(formatesqlDate);


//TODO: Test goes here...
    }


}
