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
        Path outputPath = new Path("/user/hive/warehouse/writerorc.db/orc/a");
//        StructObjectInspector inspector =
//                (StructObjectInspector) ObjectInspectorFactory
//                        .getReflectionObjectInspector(MyRow.class,
//                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

        List<String> columnNames = Lists.newArrayList();
        columnNames.add("name");
        columnNames.add("age");

        List<ObjectInspector> cloumnOI = Lists.newArrayList();

        ObjectInspector inspector1 =
                 ObjectInspectorFactory
                        .getReflectionObjectInspector(String.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        ObjectInspector inspector2 =
                ObjectInspectorFactory
                        .getReflectionObjectInspector(Integer.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        cloumnOI.add(inspector1);
        cloumnOI.add(inspector2);

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
        o1.add(String.valueOf("test7"));
        o1.add(Integer.valueOf(10));
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
