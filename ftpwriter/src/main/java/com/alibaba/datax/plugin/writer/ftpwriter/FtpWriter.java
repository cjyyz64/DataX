package com.alibaba.datax.plugin.writer.ftpwriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPConnectionClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.writer.ftpwriter.util.FtpUtils;
import com.google.common.collect.Sets;

/**
 * Created by haiwei.luo on 15-02-13.
 */
public class FtpWriter extends Writer {
	public static class Job extends Writer.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration writerSliceConfig = null;
		private FTPClient ftpClient = null;
		private Set<String> allRemoteFiles = null;

		@Override
		public void init() {
			this.writerSliceConfig = this.getPluginJobConf();
			this.validateParameter();
			this.ftpClient = FtpUtils.getFTPClient(
					this.writerSliceConfig.getString(Key.IP),
					this.writerSliceConfig.getInt(Key.PORT),
					this.writerSliceConfig.getString(Key.USERNAME),
					this.writerSliceConfig.getString(Key.PASSWORD));
			FtpUtils.makeSureDirectory(this.ftpClient,
					this.writerSliceConfig.getString(Key.PATH));
		}

		private void validateParameter() {
			this.writerSliceConfig.getNecessaryValue(Key.IP,
					FtpWriterErrorCode.REQUIRED_VALUE);
			this.writerSliceConfig.getNecessaryValue(Key.PORT,
					FtpWriterErrorCode.REQUIRED_VALUE);
			this.writerSliceConfig.getNecessaryValue(Key.USERNAME,
					FtpWriterErrorCode.REQUIRED_VALUE);
			this.writerSliceConfig.getNecessaryValue(Key.PASSWORD,
					FtpWriterErrorCode.REQUIRED_VALUE);
			this.writerSliceConfig.getNecessaryValue(Key.PATH,
					FtpWriterErrorCode.REQUIRED_VALUE);
			// TODO only support ftp types
			String type = this.writerSliceConfig.getString(Key.TYPE);
			if (StringUtils.isBlank(type)) {
				this.writerSliceConfig.set(Key.TYPE, Constant.DEFAULT_FTP_TYPE);
			} else {
				Set<String> supportedFtpType = Sets.newHashSet("ftp", "sftp");
				if (!supportedFtpType.contains(type)) {
					String message = String.format(
							"仅支持 [%s] FTP 方式 , 不支持您配置的 FTP 方式: [%s]",
							StringUtils.join(supportedFtpType, ","));
					throw DataXException.asDataXException(
							FtpWriterErrorCode.ILLEGAL_VALUE,
							String.format(message, type));
				}
			}

			UnstructuredStorageWriterUtil
					.validateParameter(this.writerSliceConfig);
		}

		@Override
		public void prepare() {
			LOG.info("begin do prepare...");
			String path = this.writerSliceConfig.getString(Key.PATH);
			String fileName = this.writerSliceConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
			String writeMode = this.writerSliceConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);

			// TODO truncate option handler, clear all files
			if ("truncate".equals(writeMode)) {
				LOG.info(String.format(
						"由于您配置了writeMode truncate, 开始清理 [%s] 下面的内容", path));
				FtpUtils.deleteDir(this.ftpClient, path);
				this.allRemoteFiles = new HashSet<String>();
				FtpUtils.makeSureDirectory(this.ftpClient,
						this.writerSliceConfig.getString(Key.PATH));
			} else if ("append".equals(writeMode)) {
				LOG.info(String
						.format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
								path, fileName));
			} else if ("error".equals(writeMode)) {
				LOG.info(String.format(
						"由于您配置了writeMode error, 开始检查 [%s] 下面的内容", path));
				this.allRemoteFiles = FtpUtils.listFiles(this.ftpClient, path);
				if (0 < this.allRemoteFiles.size()) {
					throw DataXException
							.asDataXException(
									FtpWriterErrorCode.ILLEGAL_VALUE,
									String.format(
											"您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
											path));
				}
			}

			FtpUtils.closeConnection(this.ftpClient);
		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {

		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			LOG.info("begin do split...");
			List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
			String fileName = this.writerSliceConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);

			String fileSuffix;
			for (int i = 0; i < mandatoryNumber; i++) {
				// handle same file name
				Configuration splitedTaskConfig = this.writerSliceConfig
						.clone();

				String fullFileName = null;
				fileSuffix = UUID.randomUUID().toString().replace('-', '_');
				fullFileName = String.format("%s__%s", fileName, fileSuffix);
				while (allRemoteFiles.contains(fullFileName)) {
					fileSuffix = UUID.randomUUID().toString().replace('-', '_');
					fullFileName = String
							.format("%s__%s", fileName, fileSuffix);
				}
				allRemoteFiles.add(fullFileName);

				splitedTaskConfig
						.set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
								fullFileName);

				LOG.info(String.format("splited write object name:[%s]",
						fullFileName));

				writerSplitConfigs.add(splitedTaskConfig);
			}
			LOG.info("end do split.");
			return writerSplitConfigs;
		}
	}

	public static class Task extends Writer.Task {
		private static final Logger LOG = LoggerFactory.getLogger(Task.class);

		private Configuration writerSliceConfig = null;
		private FTPClient ftpClient = null;
		private String fileName = null;
		private String path = null;

		@Override
		public void init() {
			this.writerSliceConfig = this.getPluginJobConf();
			this.path = this.writerSliceConfig.getString(Key.PATH);
			this.ftpClient = FtpUtils.getFTPClient(
					this.writerSliceConfig.getString(Key.IP),
					this.writerSliceConfig.getInt(Key.PORT),
					this.writerSliceConfig.getString(Key.USERNAME),
					this.writerSliceConfig.getString(Key.PASSWORD));
			this.fileName = this.writerSliceConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			LOG.info("begin do write...");
			String fileFullPath = this.buildFilePath();
			LOG.info(String.format("write to file : [%s]", fileFullPath));
			OutputStream outputStream = null;
			try {
				outputStream = this.ftpClient.storeFileStream(fileFullPath);
				UnstructuredStorageWriterUtil.writeToStream(lineReceiver,
						outputStream, this.writerSliceConfig, this.fileName,
						this.getTaskPluginCollector());
			} catch (FTPConnectionClosedException e) {
				throw DataXException.asDataXException(
						FtpWriterErrorCode.Write_FILE_ERROR,
						String.format("写文件[%s]FTP连接中断异常", this.fileName), e);
			} catch (IOException e) {
				throw DataXException.asDataXException(
						FtpWriterErrorCode.Write_FILE_ERROR,
						String.format("写文件 [%s] IO异常 ", this.fileName), e);
			} finally {
				FtpUtils.closeConnection(this.ftpClient);
			}
			LOG.info("end do write");
		}

		@Override
		public void prepare() {

		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {

		}

		private String buildFilePath() {
			boolean isEndWithSeparator = false;
			switch (IOUtils.DIR_SEPARATOR) {
			case IOUtils.DIR_SEPARATOR_UNIX:
				isEndWithSeparator = this.path.endsWith(String
						.valueOf(IOUtils.DIR_SEPARATOR));
				break;
			case IOUtils.DIR_SEPARATOR_WINDOWS:
				isEndWithSeparator = this.path.endsWith(String
						.valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
				break;
			default:
				break;
			}
			if (!isEndWithSeparator) {
				this.path = this.path + IOUtils.DIR_SEPARATOR;
			}
			return String.format("%s%s", this.path, this.fileName);
		}
	}
}
