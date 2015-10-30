package com.alibaba.datax.plugin.writer.hdfswriter;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
//import com.hadoop.compression.lzo.LzopCodec;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.*;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

/**
 * HdfsWriter Tester.
 *
 * @author <Authors name>
 * @since <pre>十月 17, 2015</pre>
 * @version 1.0
 */
public class HdfsWriterTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: init()
     *
     */
    @Test
    public void testInit() throws Exception {
//TODO: Test goes here... 
    }

    /**
     *
     * Method: prepare()
     *
     */
    @Test
    public void testPrepare() throws Exception {
//TODO: Test goes here... 
    }

    /**
     *
     * Method: post()
     *
     */
    @Test
    public void testPost() throws Exception {
//TODO: Test goes here... 
    }

    /**
     *
     * Method: destroy()
     *
     */
    @Test
    public void testDestroy() throws Exception {
//TODO: Test goes here... 
    }

    /**
     *
     * Method: split(int mandatoryNumber)
     *
     */
    @Test
    public void testSplitMandatoryNumber() throws Exception {
//TODO: Test goes here... 
    }

    /**
     *
     * Method: startWrite(RecordReceiver lineReceiver)
     *
     */
    @Test
    public void testStartWriteLineReceiver() throws Exception {
//TODO: Test goes here... 
    }

    /**
     *
     * Method: supportFailOver()
     *
     */
    @Test
    public void testSupportFailOver() throws Exception {
//TODO: Test goes here... 
    }


    /**
     *
     * Method: validateParameter()
     *
     */
    @Test
    public void testValidateParameter() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = HdfsWriter.getClass().getMethod("validateParameter"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    /**
     *
     * Method: buildFilePath()
     *
     */
    @Test
    public void testBuildFilePath() throws Exception {
//TODO: Test goes here... 
/* 
try { 
   Method method = HdfsWriter.getClass().getMethod("buildFilePath"); 
   method.setAccessible(true); 
   method.invoke(<Object>, <Parameters>); 
} catch(NoSuchMethodException e) { 
} catch(IllegalAccessException e) { 
} catch(InvocationTargetException e) { 
} 
*/
    }

    @Test
    public void testWriteOrc() throws Exception{

        String defaultFS ="hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);

        JobConf conf = new JobConf(hadoopConf);
        FileSystem fs = FileSystem.get(conf);
        //Path outputPath = new Path("/user/hive/warehouse/test3.db/hadfswriter/test4.orc");
        Path outputPath = new Path("/user/hive/warehouse/writerorc.db/orcfull/c");

        List<String> columnNames = Lists.newArrayList();
        columnNames.add("col1");
        columnNames.add("col2");
        columnNames.add("col3");
        columnNames.add("col4");
        columnNames.add("col5");
        columnNames.add("col6");
        columnNames.add("col7");
        columnNames.add("col8");
        columnNames.add("col9");
        columnNames.add("col10");
        columnNames.add("col11");
        columnNames.add("col12");

        List<ObjectInspector> cloumnOI = Lists.newArrayList();
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));

        StructObjectInspector inspector = (StructObjectInspector)ObjectInspectorFactory.getStandardStructObjectInspector(columnNames,cloumnOI);


        OrcSerde serde = new OrcSerde();
        Properties p = new Properties();
        p.setProperty("orc.compress", "SNAPPY");
        p.setProperty("orc.bloom.filter.columns", "\t");

        serde.initialize(conf, p);

        OutputFormat outFormat = new OrcOutputFormat();
        RecordWriter writer = outFormat.getRecordWriter(fs, conf, outputPath.toString(), Reporter.NULL);
        List<Object> o1=Lists.newArrayList();
        o1.add(Byte.valueOf("127"));
        o1.add(Short.valueOf("128"));
        o1.add(Integer.valueOf(32768));
        o1.add(Long.valueOf("2147483648"));
        o1.add(Float.valueOf("12.12"));
        o1.add(Double.valueOf(13.13));
        o1.add(String.valueOf("String"));
        o1.add(String.valueOf("varchar"));
        o1.add(String.valueOf("char"));
        o1.add(Boolean.valueOf(true));
        o1.add(Date.valueOf("2015-10-18"));
        o1.add(Timestamp.valueOf("2015-10-18 15:28:11.388"));

        writer.write(NullWritable.get(), serde.serialize(o1, inspector));
        writer.close(Reporter.NULL);
        fs.close();
        System.out.println("write success .");

    }

    @Test
    public  void testWriteText() throws Exception{
        String defaultFS ="hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);

        JobConf conf = new JobConf(hadoopConf);
        FileSystem fileSystem = FileSystem.get(conf);
        String storePath = "hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test2.textfile";
        Path path = new Path(storePath);

        FSDataOutputStream out = fileSystem.create(path);
        InputStream in = new BufferedInputStream(new FileInputStream(
                new File("/Users/shf/job/job.json")));
        byte[] b = new byte[1024];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
        System.out.println("write success .");

    }

    @Test
    public  void testWriteTextGzip() throws Exception{
        String defaultFS ="hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);

        JobConf conf = new JobConf(hadoopConf);
        FileSystem fileSystem = FileSystem.get(conf);
        //String storePath = "hdfs://10.101.204.12:9000/user/hive/warehouse/hdfswriter.db/text_table_gzip/b.gz";
        String storePath = "hdfs://10.101.204.12:9000/user/hive/warehouse/hdfswriter.db/text_table_lzo/b.lzo";
        Path path = new Path(storePath);

        FSDataOutputStream fSDataOutputStream = fileSystem.create(path);
        String  aa = "3\t128\t32768\t2147483648\t12.12\t13.13\tstring\tvarchar\tchar\ttrue\t2015-10-18\t2015-10-18 17:44:37.583";
        //String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
        String codecClassName = "com.hadoop.compression.lzo.LzopCodec";

        Class<?> codecClass = Class.forName(codecClassName);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        CompressionOutputStream compressionOutputStream = codec.createOutputStream(fSDataOutputStream);

        InputStream inputStream  =  new ByteArrayInputStream(aa.getBytes());

        IOUtils.copyBytes(inputStream, compressionOutputStream, conf);
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(compressionOutputStream);

        fileSystem.close();
        System.out.println("write success .");

    }
} 
