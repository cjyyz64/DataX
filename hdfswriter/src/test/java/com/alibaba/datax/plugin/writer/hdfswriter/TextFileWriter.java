package com.alibaba.datax.plugin.writer.hdfswriter;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;

import java.io.*;

/**
 * Created by shf on 15/10/8.
 */
public class TextFileWriter {
    private static org.apache.hadoop.conf.Configuration getHadoopConf(String defaultFS ){
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);
        return hadoopConf;
    }

    public static void main(String args[]) throws IOException {
        String defaultFS = "hdfs://10.101.204.12:9000";
        org.apache.hadoop.conf.Configuration hadoopConf = getHadoopConf(defaultFS);
        JobConf conf = new JobConf(hadoopConf);
        FileSystem fileSystem = FileSystem.get(conf);

        //String storePath = "/user/hive/warehouse/writer.db/text/test.textfile";
        String storePath = "/user/hive/warehouse/writer.db/text";
        //String storePath = "hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test2.textfile";
        Path path = new Path(storePath);
        System.out.println(fileSystem.exists(path));
        System.out.println(fileSystem.isDirectory(path));
        PathFilter pathFilter = new GlobFilter("qiran"+"__*");
        FileStatus[] status = fileSystem.listStatus(path,pathFilter);
        System.out.println("int".toUpperCase());
        for(int i=0;i<status.length;i++){
            //FileStatus{path=hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile; isDirectory=false; length=1587; replication=3; blocksize=134217728; modification_time=1444297813258; access_time=1444297812616; owner=shf; group=supergroup; permission=rw-r--r--; isSymlink=false}
            System.out.println(status[i].getPath().toString());
        }
//
//        if (fileSystem.exists(path)) {
//            System.out.println("File " + storePath + " already exists");
//            return;
//        }

//        FSDataOutputStream out = fileSystem.create(path);
//        InputStream in = new BufferedInputStream(new FileInputStream(
//                new File("/Users/shf/job/job.json")));
//        byte[] b = new byte[1024];
//        int numBytes = 0;
//        while ((numBytes = in.read(b)) > 0) {
//            out.write(b, 0, numBytes);
//        }
//
//        in.close();
//        out.close();
          fileSystem.close();
    }

}
