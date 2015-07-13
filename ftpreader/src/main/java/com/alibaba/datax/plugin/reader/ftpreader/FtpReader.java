package com.alibaba.datax.plugin.reader.ftpreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.google.common.collect.Sets;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 
 * @ClassName: FtpFileReader
 * @Description: TODO
 * @author hanfa.shf@alibaba-inc.com
 * @date 2015年7月6日 上午9:24:57
 *
 */
public class FtpReader extends Reader {
	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration originConfig = null;

		private List<String> path = null;

		private HashSet<String> sourceFiles;

		// ftp链接参数
		private String host;
		private int port;
		private String username;
		private String password;
		private String protocol;
		private int timeout;

		SftpUtil sftpUtil = null;
		private ChannelSftp sftp = null;;

		FtpUtil ftpUtil = null;
		FTPClient ftpClient = null;

		@Override
		public void init() {
			this.originConfig = this.getPluginJobConf();
			this.sourceFiles = new HashSet<String>();
			this.validateParameter();

			// sftp链接参数初始化
			this.host = originConfig.getString(Key.HOST);
			this.protocol = originConfig.getString(Key.PROTOCOL);
			this.port = originConfig.getInt(Key.PORT);
			this.username = originConfig.getString(Key.USERNAME);
			this.password = originConfig.getString(Key.PASSWORD);
			this.timeout = originConfig.getInt(Key.TIMEOUT, 30000);

			// 建立连接
			if ("sftp".equals(protocol)) {
				this.sftpUtil = SftpUtil.getInstance();
				try {
					sftp = sftpUtil.getChannel(host, username, password, port, timeout);
				} catch (JSchException e) {
					// TODO Auto-generated catch block
					String message = String.format("无法通过sftp与服务器建立链接 : [%s]",
							"Connected failed to ftp server by sftp, message:host =" + host + ",username = " + username
									+ ",password = " + password + ",port =" + port);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_CONNEECT_FTPSERVER, message, e);
				}
			} else if ("ftp".equals(protocol)) {
				// ftp 协议
				this.ftpUtil = FtpUtil.getInstance();
				ftpClient = ftpUtil.connectServer(host, username, password, port, timeout);

			}

		}

		private void validateParameter() {
			String host = this.originConfig.getNecessaryValue(Key.HOST, FtpReaderErrorCode.REQUIRED_VALUE);
			if (StringUtils.isBlank(host)) {
				throw DataXException.asDataXException(FtpReaderErrorCode.REQUIRED_VALUE, "您需要指定ftp服务器地址");
			}

			String username = this.originConfig.getNecessaryValue(Key.USERNAME, FtpReaderErrorCode.REQUIRED_VALUE);
			if (StringUtils.isBlank(username)) {
				throw DataXException.asDataXException(FtpReaderErrorCode.REQUIRED_VALUE, "您需要指定登陆ftp服务器的用户名");
			}
			String password = this.originConfig.getNecessaryValue(Key.PASSWORD, FtpReaderErrorCode.REQUIRED_VALUE);
			if (StringUtils.isBlank(password)) {
				throw DataXException.asDataXException(FtpReaderErrorCode.REQUIRED_VALUE, "您需要指定登陆ftp服务器的密码");
			}

			// Compatible with the old version, path is a string before
			String pathInString = this.originConfig.getNecessaryValue(Key.PATH, FtpReaderErrorCode.REQUIRED_VALUE);
			if (StringUtils.isBlank(pathInString)) {
				throw DataXException.asDataXException(FtpReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
			}
			if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
				path = new ArrayList<String>();
				path.add(pathInString);
			} else {
				path = this.originConfig.getList(Key.PATH, String.class);
				if (null == path || path.size() == 0) {
					throw DataXException.asDataXException(FtpReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
				}
			}

			String encoding = this.originConfig.getString(
					com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING,
					com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_ENCODING);
			if (StringUtils.isBlank(encoding)) {
				this.originConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING, null);
			} else {
				try {
					encoding = encoding.trim();
					this.originConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING, encoding);
					Charsets.toCharset(encoding);
				} catch (UnsupportedCharsetException uce) {
					throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
							String.format("不支持您配置的编码格式 : [%s]", encoding), uce);
				} catch (Exception e) {
					throw DataXException.asDataXException(FtpReaderErrorCode.CONFIG_INVALID_EXCEPTION,
							String.format("编码配置异常, 请联系我们: %s", e.getMessage()), e);
				}
			}

			// column: 1. index type 2.value type 3.when type is Date, may have
			// format
			List<Configuration> columns = this.originConfig
					.getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
			// handle ["*"]
			if (null != columns && 1 == columns.size()) {
				String columnsInStr = columns.get(0).toString();
				if ("\"*\"".equals(columnsInStr) || "'*'".equals(columnsInStr)) {
					this.originConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN, null);
					columns = null;
				}
			}

			if (null != columns && columns.size() != 0) {
				for (Configuration eachColumnConf : columns) {
					eachColumnConf.getNecessaryValue(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE,
							FtpReaderErrorCode.REQUIRED_VALUE);
					Integer columnIndex = eachColumnConf
							.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.INDEX);
					String columnValue = eachColumnConf
							.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.VALUE);

					if (null == columnIndex && null == columnValue) {
						throw DataXException.asDataXException(FtpReaderErrorCode.NO_INDEX_VALUE,
								"由于您配置了type, 则至少需要配置 index 或 value");
					}

					if (null != columnIndex && null != columnValue) {
						throw DataXException.asDataXException(FtpReaderErrorCode.MIXED_INDEX_VALUE,
								"您混合配置了index, value, 每一列同时仅能选择其中一种");
					}
					if (null != columnIndex && columnIndex < 0) {
						throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
								String.format("index需要大于等于0, 您配置的index为[%s]", columnIndex));
					}
				}
			}

			// only support compress types
			String compress = this.originConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS);
			if (StringUtils.isBlank(compress)) {
				this.originConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS, null);
			} else {
				Set<String> supportedCompress = Sets.newHashSet("gzip", "bzip2");
				compress = compress.toLowerCase().trim();
				if (!supportedCompress.contains(compress)) {
					throw DataXException.asDataXException(FtpReaderErrorCode.ILLEGAL_VALUE,
							String.format("仅支持 gzip, bzip2 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]", compress));
				}
				this.originConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COMPRESS, compress);
			}

			String delimiterInStr = this.originConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FIELD_DELIMITER);
			// warn: if have, length must be one
			if (null != delimiterInStr && 1 != delimiterInStr.length()) {
				throw DataXException.asDataXException(UnstructuredStorageReaderErrorCode.ILLEGAL_VALUE,
						String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
			}

		}

		@Override
		public void prepare() {
			LOG.debug("prepare() begin...");
			// warn:make sure this regex string
			// warn:no need trim

			// this.sourceFiles = originConfig.getList("path", String.class);
			if ("sftp".equals(protocol)) {
				sftpUtil.listFiles(sftp, this.path, this.sourceFiles);
			}else if("ftp".equals(protocol)){
				ftpUtil.listFiles(ftpClient, this.path, this.sourceFiles);
			}			

			LOG.info(String.format("您即将读取的文件数为: [%s]", this.sourceFiles.size()));
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
			if ("sftp".equals(protocol)) {
				sftpUtil.closeChannel(sftp);
			}else if("ftp".equals(protocol)){
				/*try {
					ftpClient.completePendingCommand();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				ftpUtil.closeServer(ftpClient);
			}
		}

		// warn: 如果源目录为空会报错，拖空目录意图=>空文件显示指定此意图
		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.debug("split() begin...");
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

			// warn:每个slice拖且仅拖一个文件,
			// int splitNumber = adviceNumber;
			int splitNumber = this.sourceFiles.size();
			if (0 == splitNumber) {
				throw DataXException.asDataXException(FtpReaderErrorCode.EMPTY_DIR_EXCEPTION,
						String.format("未能找到待读取的文件,请确认您的配置项path: %s", this.originConfig.getString(Key.PATH)));
			}

			List<List<String>> splitedSourceFiles = this.splitSourceFiles(new ArrayList(this.sourceFiles), splitNumber);
			for (List<String> files : splitedSourceFiles) {
				Configuration splitedConfig = this.originConfig.clone();
				splitedConfig.set(Constant.SOURCE_FILES, files);
				readerSplitConfigs.add(splitedConfig);
			}
			LOG.debug("split() ok and end...");
			return readerSplitConfigs;
		}

		private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
			List<List<T>> splitedList = new ArrayList<List<T>>();
			int averageLength = sourceList.size() / adviceNumber;
			averageLength = averageLength == 0 ? 1 : averageLength;

			for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
				end = begin + averageLength;
				if (end > sourceList.size()) {
					end = sourceList.size();
				}
				splitedList.add(sourceList.subList(begin, end));
			}
			return splitedList;
		}

	}

	public static class Task extends Reader.Task {
		private static Logger LOG = LoggerFactory.getLogger(Task.class);

		private String host;
		private int port;
		private String username;
		private String password;
		private String protocol;
		private int timeout;

		private Configuration readerSliceConfig;
		private List<String> sourceFiles;

		private SftpUtil sftpUtil = null;
		private ChannelSftp sftp = null;;

		private FtpUtil ftpUtil = null;
		private FTPClient ftpClient = null;

		@Override
		public void init() {
			/* for ftp connection */
			this.readerSliceConfig = this.getPluginJobConf();
			this.host = readerSliceConfig.getString(Key.HOST);
			this.protocol = readerSliceConfig.getString(Key.PROTOCOL);
			this.port = readerSliceConfig.getInt(Key.PORT);
			this.username = readerSliceConfig.getString(Key.USERNAME);
			this.password = readerSliceConfig.getString(Key.PASSWORD);
			this.timeout = readerSliceConfig.getInt(Key.TIMEOUT, 30000);

			// this.readerSliceConfig.getBool("a[0]", true);
			// this.timeout = readerSliceConfig.getInt(Key.TIMEOUT);

			this.sourceFiles = this.readerSliceConfig.getList(Constant.SOURCE_FILES, String.class);

			if ("sftp".equals(protocol)) {
					this.sftpUtil = SftpUtil.getInstance();
				try {
					sftp = sftpUtil.getChannel(host, username, password, port, timeout);
				} catch (JSchException e) {
					// TODO Auto-generated catch block
					String message = String.format("无法通过sftp与服务器建立链接 : [%s]",
							"Connected failed to ftp server by sftp, message:host =" + host + ",username = " + username
									+ ",password = " + password + ",port =" + port);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_CONNEECT_FTPSERVER, message, e);
				}
			} else if ("ftp".equals(protocol)) {
				// ftp 协议
				this.ftpUtil = FtpUtil.getInstance();
				ftpClient = ftpUtil.connectServer(host, username, password, port, timeout);
				
			}

		}

		@Override
		public void prepare() {

		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {			
			if ("sftp".equals(protocol)) {
				sftpUtil.closeChannel(sftp);
			}else if("ftp".equals(protocol)){
				try {
					ftpClient.completePendingCommand();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				ftpUtil.closeServer(ftpClient);
			}

		}

		@Override
		public void startRead(RecordSender recordSender) {
			LOG.debug("start read source files...");
			for (String fileName : this.sourceFiles) {
				LOG.info(String.format("reading file : [%s]", fileName));
				InputStream inputStream=null;
				try {
					// inputStream = new FileInputStream(fileName);
					if ("sftp".equals(protocol)) {
						inputStream = sftp.get(fileName);
					}else if ("ftp".equals(protocol)) {
						inputStream =ftpClient.retrieveFileStream(fileName);
					}
					
					UnstructuredStorageReaderUtil.readFromStream(inputStream, fileName, this.readerSliceConfig,
							recordSender, this.getTaskPluginCollector());
					recordSender.flush();
				} catch (SftpException e) {
					// warn: sock 文件无法read,能影响所有文件的传输,需要用户自己保证
					String message = String.format("找不到待读取的文件 : [%s]", fileName);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.OPEN_FILE_ERROR, message);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			LOG.debug("end read source files...");
		}

	}
}
