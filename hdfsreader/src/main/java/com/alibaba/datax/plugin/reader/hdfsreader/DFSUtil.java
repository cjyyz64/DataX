package com.alibaba.datax.plugin.reader.hdfsreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil {

    private Configuration conf = null;

    public DFSUtil(String defaultFS){
        conf = new Configuration();
        conf.set("fs.defaultFS", defaultFS);
    }

    private HashSet<String> sourceHDFSAllFilesList = new HashSet<String>();

    public HashSet<String> getHDFSAllFiles(String hdfsPath){

        InputStream in = null;

        try {

            // 读取HDFS上的文件系统，获取到的是一个org.apache.hadoop.hdfs.DistributedFileSystem对象
            FileSystem hdfs = FileSystem.get(URI.create(hdfsPath), conf);

            // 使用缓冲流，进行按行读取的功能
            BufferedReader buff = null;

            // 获取要读取的文件的根目录
            Path listFiles = new Path(hdfsPath);

            // 获取要读取的文件的根目录的所有二级子文件目录
            FileStatus stats[] = hdfs.listStatus(listFiles);

            for (FileStatus stat : stats) {
                // 判断是不是目录，如果是目录，递归调用
                if (stat.isDirectory()) {
                    getHDFSAllFiles(stat.getPath().toString());
                } else {
                    String pathFile = stat.getPath().toString();
                    System.out.println("文件路径名:" + pathFile);
                    sourceHDFSAllFilesList.add(pathFile);
                }
            }
            return sourceHDFSAllFilesList;
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }

    public InputStream getInputStream(String filepath){
        InputStream inputStream = null;
        Path path = new Path(filepath);
        try{
            FileSystem fs = FileSystem.get(conf);
            inputStream = fs.open(path);
            return inputStream;
        }catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void readfile(String filepath,String defaultFS){
        Path path = new Path(filepath);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", defaultFS);
        try{
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
