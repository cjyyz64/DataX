package com.alibaba.datax.plugin.writer.hdfswriter;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
//import com.hadoop.compression.lzo.LzopCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.*;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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
        Path outputPath = new Path("/user/hive/warehouse/writerorc.db/orcfull/e");

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
    public void testWriteOrc2() throws Exception{

        String defaultFS ="hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);

        JobConf conf = new JobConf(hadoopConf);
        FileSystem fs = FileSystem.get(conf);
        //Path outputPath = new Path("/user/hive/warehouse/test3.db/hadfswriter/test4.orc");
        Path outputPath = new Path("/user/hive/warehouse/hdfswriter.db/orc_table_snappy/e");

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

        FileOutputFormat outFormat = new OrcOutputFormat();
        RecordWriter writer = outFormat.getRecordWriter(fs, conf, outputPath.toString(), Reporter.NULL);
        AcidOutputFormat.Options options = new AcidOutputFormat.Options(hadoopConf);

       // outFormat.setCompressOutput();
        outFormat.setOutputCompressorClass(conf,org.apache.hadoop.io.compress.SnappyCodec.class);


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

    @Test
    public void testCSVTextWriter() throws  Exception{
        String defaultFS ="hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);

        JobConf conf = new JobConf(hadoopConf);

        FileSystem fs = FileSystem.get(conf);
        //Path outputPath = new Path("/user/hive/warehouse/test3.db/hadfswriter/test4.orc");
        Path outputPath = new Path("/user/hive/warehouse/hdfswriter.db/csv_table/csv");
        //fs.create(outputPath);

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

        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));


        StructObjectInspector inspector = (StructObjectInspector)ObjectInspectorFactory.getStandardStructObjectInspector(columnNames,cloumnOI);


        OpenCSVSerde openCSVSerde = new OpenCSVSerde();
        Properties props = new Properties();
        props.setProperty(serdeConstants.LIST_COLUMNS, "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12");
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "TINYINT,SMALLINT,INT,BIGINT,FLOAT,DOUBLE,STRING,VARCHAR,CHAR,BOOLEAN,date,TIMESTAMP");
        openCSVSerde.initialize(conf,props);

        String attempt = "attempt_200707121733_0001_m_000000_0";
        conf.set(JobContext.TASK_ATTEMPT_ID, attempt);

       // props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string,string");

        FileOutputFormat outFormat = new TextOutputFormat();

        outFormat.setOutputPath(conf,outputPath);
        outFormat.setWorkOutputPath(conf, outputPath);

        RecordWriter writer = outFormat.getRecordWriter(fs, conf, outputPath.toString(), Reporter.NULL);
        //AcidOutputFormat.Options options = new AcidOutputFormat.Options(hadoopConf);
        //options.
//        outFormat.setCompressOutput();
//        outFormat.setOutputCompressorClass();
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

        writer.write(NullWritable.get(), openCSVSerde.serialize(o1, inspector));
        writer.close(Reporter.NULL);
        fs.close();
        System.out.println("write success .");

    }


    @Test
    public void testTextOutputWriter() throws  Exception{
        String defaultFS ="hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);
        JobConf conf = new JobConf(hadoopConf);
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path("/user/hive/warehouse/hdfswriter.db/text_table/csv6");

        //String attempt = "attempt_200707121733_0001_m_000000_0";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        String attempt = "attempt_"+dateFormat.format(new java.util.Date())+"_0001_m_000000_0";
        conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
        FileOutputFormat outFormat = new TextOutputFormat();
        outFormat.setOutputPath(conf,outputPath);
        outFormat.setWorkOutputPath(conf, outputPath);

        AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf);
        outFormat.setOutputCompressorClass(conf,org.apache.hadoop.io.compress.BZip2Codec.class);


        RecordWriter writer = outFormat.getRecordWriter(fs, conf, outputPath.toString(), Reporter.NULL);
        Text text = new Text("127\t128\t32768\t2147483648\t12.12\t13.13\tString\tvarchar\tchar\ttrue\t2015-10-18\t2015-10-18 15:28:11.388");
        Text text2 = new Text("127\t128\t32768\t2147483648\t12.12\t13.13\tString\tvarchar\tchar\ttrue\t2015-10-18\t2015-10-18 15:28:11.388");

        writer.write(NullWritable.get(),text );
        writer.write(NullWritable.get(),text2 );
        writer.close(Reporter.NULL);
        fs.close();
        System.out.println("write success .");

    }
    @Test
    public void testRename() throws  Exception{
        String defaultFS ="hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);
        JobConf conf = new JobConf(hadoopConf);
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path("hdfs://10.101.204.12:9000/user/hive/warehouse/hdfswriter.db/orc_table__2e68bb45_fc09_439f_8f87_f82c35d7fcbb/qiran2__04112936_b482_4245_8e41_e71f53867482");
        Path dstPath = new Path("hdfs://10.101.204.12:9000/user/hive/warehouse/hdfswriter.db/orc_table/qiran2__04112936_b482_4245_8e41_e71f53867482");
        boolean a =fs.rename(srcPath,dstPath);
        fs.close();
        if(a) {
            System.out.println("rename success .");
        }

    }
} 
