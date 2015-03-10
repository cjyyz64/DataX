package com.alibaba.datax.plugin.writer.hbasebulkwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.hadoop.compression.lzo.LzoCodec;
import com.taobao.diamond.client.Diamond;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class HBaseHelper {

	private static final Logger LOG = LoggerFactory
			.getLogger(HBaseHelper.class);

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
      throw DataXException.asDataXException(BulkWriterError.RUNTIME,"Check the parameter of fs.default.name in hdfs-site.xml, now is "
    + uri);
    }
  }

  public static void checkHdfsVersion(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    fs.getStatus();
  }

  public static void checkTmpOutputDir(Configuration conf, String outputDir)
			throws IOException {
    if (outputDir == null || outputDir.trim().length() == 0) {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME,"Check the value of output parameter in job xml.");
    }

		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(outputDir);
		if (!fs.exists(path)) {
			fs.mkdirs(path);
		} else {
			throw DataXException.asDataXException(BulkWriterError.RUNTIME,String.format(
					"Clear up the dir %s created by tasks before.", outputDir));
		}
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
	}

	public static void loadNativeLibrary() {
    try {
      System.load(HBaseConsts.PLUGIN_HOME + "/libs/native/libhadoop.so");
      System.load(HBaseConsts.PLUGIN_HOME + "/libs/native/liblzo2.so.2");
    } catch (UnsatisfiedLinkError e) {
      if (e.getMessage().contains("already loaded in another classloader")) {
        LOG.info("Native libraries already loaded.");
      } else {
        throw DataXException.asDataXException(BulkWriterError.RUNTIME,e);
      }
    }
	}

  public static void checkNativeLzoLibrary(Configuration conf) {
    if (!LzoCodec.isNativeLzoLoaded(conf)) {
      throw DataXException.asDataXException(BulkWriterError.RUNTIME,"Load native-lzo failed!");
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
      for (Map.Entry<String, String> kv: tmpConf) {
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

  public static Configuration getConfiguration(String hdfsConfPath, String hbaseConfPath, String clusterId) {
    Configuration conf = new Configuration();
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
        throw DataXException.asDataXException(BulkWriterError.RUNTIME,e);
      }
    }

    return conf;
  }
}
