package com.alibaba.datax.plugin.reader.hdfsreader;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil {
    public HashSet<String> getAllFiles(String srcPath, int parentLevel, int maxTraversalLevel){
        HashSet<String> sourceAllFiles = new HashSet<String>();
        if (!srcPath.isEmpty()) {
            sourceAllFiles.add(srcPath);
        }
        return sourceAllFiles;
    }

    public InputStream getInputStream(String filepath,String defaultFS){
        InputStream inputStream = null;
        Path path = new Path(filepath);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", defaultFS);
        try{
            FileSystem fs = FileSystem.get(conf);
            inputStream = fs.open(path);
            return inputStream;
        }catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
