package com.alibaba.datax.plugin.reader.hdfsreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsReader.Job.class);

    private Configuration conf = null;

    public DFSUtil(String defaultFS){
        conf = new Configuration();
        conf.set("fs.defaultFS", defaultFS);
    }

    private HashSet<String> sourceHDFSAllFilesList = new HashSet<String>();

    public HashSet<String> getHDFSAllFiles(String hdfsPath){
        try {
            FileSystem hdfs = FileSystem.get(conf);
            //判断hdfsPath是否包含正则符号
            if(hdfsPath.contains("*") || hdfsPath.contains("?")){
                Path path = new Path(hdfsPath);
                FileStatus stats[] = hdfs.globStatus(path);
                for(FileStatus f : stats){
                    if(f.isFile()){
                        sourceHDFSAllFilesList.add(f.getPath().toString());
                    }
                    else if(f.isDirectory()){
                        getHDFSALLFiles_NO_Regex(f.getPath().toString(), hdfs);
                    }
                }
            }
            else{
                getHDFSALLFiles_NO_Regex(hdfsPath, hdfs);
            }
            return sourceHDFSAllFilesList;
        }catch (IOException e){
            String message = String.format("无法读取路径[%s]下的所有文件,请确认您的配置项path是否正确，且配置的用户有权限进入"
                    , hdfsPath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.PATH_CONFIG_ERROR, message);
        }
    }

    private HashSet<String> getHDFSALLFiles_NO_Regex(String path,FileSystem hdfs) throws IOException{

        // 获取要读取的文件的根目录
        Path listFiles = new Path(path);

        // 获取要读取的文件的根目录的所有二级子文件目录
        FileStatus stats[] = hdfs.listStatus(listFiles);

        for (FileStatus f : stats) {
            // 判断是不是目录，如果是目录，递归调用
            if (f.isDirectory()) {
                getHDFSALLFiles_NO_Regex(f.getPath().toString(),hdfs);
            } else {
                sourceHDFSAllFilesList.add(f.getPath().toString());
            }
        }
        return sourceHDFSAllFilesList;
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
