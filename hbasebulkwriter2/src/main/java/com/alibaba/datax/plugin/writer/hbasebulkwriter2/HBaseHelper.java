package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import com.google.common.base.Strings;
import com.taobao.diamond.client.Diamond;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.compress.LzoCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

public class HBaseHelper {

    private static final Logger LOG = LoggerFactory
            .getLogger(HBaseHelper.class);

    private static final String USERID_SKYNET = System.getenv("SKYNET_ONDUTY_WORKNO");

    public static HFileWriter createHFileWriter(HTable htable, Configuration conf,
                                                String outputDir) throws IOException {
        conf.set("hbase.mapreduce.hfileoutputformat.datablock.encoding", "DIFF");
        conf.set("hfile.compression", "lzo");
        conf.set("hfile.compression.turnoff", "false");
        conf.set("hbase.hregion.max.filesize", "53687091200");
        HFileWriter.configureBloomType(htable, conf);
        HFileWriter.configureCompression(htable, conf);
        HFileWriter writer = new HFileWriter(outputDir, conf);
        return writer;
    }

    public static void disableLogger() {
        org.apache.log4j.Logger.getRootLogger().setLevel(
                org.apache.log4j.Level.ERROR);

        Logger logger = org.slf4j.LoggerFactory
                .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        if (logger instanceof ch.qos.logback.classic.Logger) {
            ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) logger;
            root.setLevel(ch.qos.logback.classic.Level.ERROR);
        }
    }

    public static void enableLogger() {
        org.apache.log4j.Logger.getRootLogger().setLevel(
                org.apache.log4j.Level.INFO);

        Logger logger = org.slf4j.LoggerFactory
                .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        if (logger instanceof ch.qos.logback.classic.Logger) {
            ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) logger;
            root.setLevel(ch.qos.logback.classic.Level.INFO);
        }
    }

    public static void checkConf(Configuration conf) {
        String uri = conf.get("fs.default.name");
        if (!uri.startsWith("hdfs")) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME, "fs.default.name in hdfs-site.xml参数错误，请联系HBase PE!, now is "
                    + uri);
        }
    }

    public static void checkHdfsVersion(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.getStatus();
    }

    public static void checkTmpOutputDir(final Configuration conf, final String outputDir, int retryTimes) {

        try {
            RetryUtil.executeWithRetry(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    FileSystem fs = FileSystem.get(conf);
                    Path path = new Path(outputDir);

                    if (!fs.exists(path)) {
                        fs.mkdirs(path);
                    }
                    return 0;
                }
            }, retryTimes, 1, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(BulkWriterError.IO, "请联系hbase同学检查hadoop集群", e);
        }


        LOG.info("checkTmpOutputDir => " + outputDir);
    }

    public static String checkOutputDirAndMake(Configuration conf, String outputDir)
            throws IOException {
        if (outputDir == null || outputDir.trim().length() == 0) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME, "DATAX FATAL! 没有生成有效的配置(缺少hbase_output),请联系askdatax");
        }

        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(outputDir);

        Random random = new Random();
        //一直重试下去
        while (fs.exists(path)) {
            LOG.info("checkOutputDirAndMake# {} 目录已经存在，尝试创建新的目录... ", outputDir);
            outputDir = outputDir + "_" + random.nextInt(100000);
            path = new Path(outputDir);
        }
        fs.mkdirs(path);
        LOG.info("checkOutputDirAndMake# 使用的目录为{} ", outputDir);
        return outputDir;
    }

    @SuppressWarnings("deprecation")
    public static void clearTmpOutputDir(Configuration conf, String outputDir) {
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(outputDir));
        } catch (IOException e) {
            LOG.error(
                    String.format("Clean tmpOutputDir %s failed!", outputDir),
                    e);
        }

        LOG.info("clearTmpOutputDir# 目标目录{}", outputDir);
    }

    public static void loadNativeLibrary() {
        try {
            System.load(HBaseConsts.PLUGIN_HOME + "/libs/native/libhadoop.so");
            System.load(HBaseConsts.PLUGIN_HOME + "/libs/native/liblzo2.so.2");
        } catch (UnsatisfiedLinkError e) {
            if (e.getMessage().contains("already loaded in another classloader")) {
                LOG.info("Native libraries already loaded.");
            } else {
                throw DataXException.asDataXException(BulkWriterError.RUNTIME, e);
            }
        }
    }

    public static void checkNativeLzoLibrary(Configuration conf) {
        if (!LzoCodec.isNativeLzoLoaded(conf)) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME, "Load native-lzo failed!");
        }
    }

    private static Configuration getConfigurationByClusterIdAndDataId(Configuration conf, String clusterId, String dataId) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }

        if (StringUtils.isNotEmpty(clusterId)) {
            String confStr = Diamond.getConfig(dataId, HBaseConsts.DIAMOND_GROUP,
                    HBaseConsts.DIAMOND_TIMEOUT);
            if (StringUtils.isEmpty(confStr)) {
                throw new IOException(String.format("the %s in Diamond is empty.", dataId));
            }
            InputStream in = IOUtils.toInputStream(confStr);

            // Because the bug of HADOOP-7614
            Configuration tmpConf = new Configuration();
            tmpConf.addResource(in);
            for (Map.Entry<String, String> kv : tmpConf) {
                conf.set(kv.getKey(), kv.getValue());
            }
        }

        return conf;
    }

    private static Configuration getConfigurationByClusterId(Configuration conf, String clusterId) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }

        if (StringUtils.isNotEmpty(clusterId)) {
            String dataId = String.format(HBaseConsts.DIAMOND_DATA_ID_HBASE_CONF, clusterId);
            conf = getConfigurationByClusterIdAndDataId(conf, clusterId, dataId);

            dataId = String.format(HBaseConsts.DIAMOND_DATA_ID_HDFS_CONF, clusterId);
            conf = getConfigurationByClusterIdAndDataId(conf, clusterId, dataId);
        }

        return conf;
    }

    public static Configuration getConfiguration(String hdfsConfPath, String hbaseConfPath, String clusterName, String hmcAddress, String clusterId) {
        Configuration conf = new Configuration();

        if (Strings.isNullOrEmpty(clusterName)) {
            if (StringUtils.isNotEmpty(hdfsConfPath)) {
                conf.addResource(new Path(hdfsConfPath));
            }

            if (StringUtils.isNotEmpty(hbaseConfPath)) {
                conf.addResource(new Path(hbaseConfPath));
            }

            if (StringUtils.isNotEmpty(clusterId)) {
                try {
                    conf = getConfigurationByClusterId(conf, clusterId);
                } catch (Exception e) {
                    LOG.error("Get cluster configuration file from diamond failed.", e);
                    throw DataXException.asDataXException(BulkWriterError.RUNTIME, e);
                }
            }
        } else {
            Map<String, String> mapConf = getConfFromHMCWithRetry(clusterName, hmcAddress);
            for (Map.Entry<String, String> entry : mapConf.entrySet()) {
                LOG.info("hbase conf value: " + entry.getKey() + " => " + entry.getValue());
                conf.set(entry.getKey(), entry.getValue());
            }
        }


        return conf;
    }

    public static Map<String, String> getConfFromHMCWithRetry(final String clusterName, final String hmcAddress) {

        Map<String, String> res = null;
        try {
            res = RetryUtil.executeWithRetry(new Callable<Map<String, String>>() {
                @Override
                public Map<String, String> call() throws Exception {
                    return getConfFromHMC(clusterName, hmcAddress);
                }
            }, 15, 1, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME, "获取hdfs目录失败，请联系hbase同学检查hdfs集群", e);
        }
        return res;
    }

    public static Map<String, String> getConfFromHMC(String clusterName, String hmcAddress) throws Exception {


        if (Strings.isNullOrEmpty(hmcAddress)) {
            LOG.warn("hmcAddress为空，尝试使用默认值! ");
            hmcAddress = "http://socs.alibaba-inc.com/open/api/hbase/getHbaseConfig";
        }
        if (hmcAddress.endsWith("/")) {
            hmcAddress = hmcAddress.substring(0, hmcAddress.length());
        }

        String userId = USERID_SKYNET;
        if (Strings.isNullOrEmpty(userId)) {
            LOG.warn("环境变量SKYNET_ONDUTY_WORKNO为空，尝试使用默认值");
            userId = "068141";
        }

        String guid = "guid=" + "hbase." + clusterName + ".table";


        String urlstr = hmcAddress + "?" + guid + "&userId=" + userId;
        HttpURLConnection conn = null;
        try {
            URL url = new URL(urlstr);

            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(20000);
            conn.setReadTimeout(60000);
            conn.setDoInput(true);
            conn.connect();

            InputStream in = conn.getInputStream();
            String result = IOUtils.toString(in, "UTF-8");
            System.out.println("=========> " + conn.getResponseCode());
            System.out.println("=========> " + result);



            int resCode = conn.getResponseCode();
            if (resCode != 200) {
                throw new Exception(String.format("从HMC获取conf出错(code=%s)，url=%s,message=%s", resCode, urlstr, result));
            }else{
                com.alibaba.datax.common.util.Configuration config = com.alibaba.datax.common.util.Configuration.from(result);
                Map<String,String> map1=config.get("data.hbaseSite",Map.class);
                Map<String,String> map2=config.get("data.hdfsSite",Map.class);
                map1.putAll(map2);
                System.out.println("=========> " + map1);
                return map1;
            }

        } catch (Exception e) {
            LOG.error("从HMC获取conf出错,Exception({})", e.getMessage(), e);
            throw e;
        } finally {
            if (conn != null) {
                IOUtils.close(conn);
            }
        }

    }
}