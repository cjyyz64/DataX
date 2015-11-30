package com.alibaba.datax.plugin.writer.hdfswriter;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;


/**
 * Created by shf on 15/9/22.
 */
public class OrcFileWriter {


    public static void main(String args[]) throws IOException{
        String defaultFS ="hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConf(defaultFS);

//        FileSystem fs = null;
//        try {
//            fs = FileSystem.get(hadoopConf);
//            fs.mkdirs(new Path("/hdfswriter/"));
//            fs.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        JobConf conf = new JobConf(hadoopConf);
        FileSystem fs = FileSystem.get(conf);
        //Path outputPath = new Path("/user/hive/warehouse/test3.db/hadfswriter/test4.orc");
        //Path outputPath = new Path("/user/hive/warehouse/writerorc.db/orcfull/a");
        Path outputPath = new Path("/b");
//        StructObjectInspector inspector =
//                (StructObjectInspector) ObjectInspectorFactory
//                        .getReflectionObjectInspector(MyRow.class,
//                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

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

//        ObjectInspector inspector1 =
//                 ObjectInspectorFactory
//                        .getReflectionObjectInspector(String.class,
//                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
//        ObjectInspector inspector2 =
//                ObjectInspectorFactory
//                        .getReflectionObjectInspector(Integer.class,
//                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
//        cloumnOI.add(inspector1);
//        cloumnOI.add(inspector2);
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
//        cloumnOI.add(ObjectInspectorFactory.getReflectionObjectInspector(BigDecimal.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
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
//        StructObjectInspector inspector = null;
//        try {
//            inspector = (StructObjectInspector) serde.getObjectInspector();
//        } catch (SerDeException e) {
//            e.printStackTrace();
//        }
        OutputFormat outFormat = new OrcOutputFormat();
        RecordWriter writer = outFormat.getRecordWriter(fs, conf,
                outputPath.toString(), Reporter.NULL);
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
//        writer.write(NullWritable.get(), serde.serialize(new Object[]{"李四",22}, inspector));
//        writer.write(NullWritable.get(), serde.serialize(new MyRow("王五",30), inspector));
        writer.close(Reporter.NULL);
        fs.close();
        System.out.println("write success .");
    }

    static class MyRow implements Writable {
        Object[] data;

        MyRow(Object[] data) {
            this.data = data;
        }

        @Override
        public void readFields(DataInput arg0) throws IOException {
            throw new UnsupportedOperationException("no write");
        }

        @Override
        public void write(DataOutput arg0) throws IOException {
            throw new UnsupportedOperationException("no read");
        }

    }



    private static org.apache.hadoop.conf.Configuration getHadoopConf(String defaultFS ){
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);
        return hadoopConf;
    }




}
