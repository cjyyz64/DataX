package com.alibaba.datax.plugin.writer.hbasebulkwriter.tools;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.alibaba.datax.common.util.Configuration;

public class DataxTest {
   public static void main(String[] args) throws IOException {
     InputStream is = DataxTest.class.getResourceAsStream("/job.json");     
     System.out.println(is.available());
     Configuration conf = Configuration.from(new File("/home/admin/datax/datax/job/job.json"));
     System.out.println(conf.toJSON());
   }
}
