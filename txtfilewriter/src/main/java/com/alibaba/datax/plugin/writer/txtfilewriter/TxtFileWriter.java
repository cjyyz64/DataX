package com.alibaba.datax.plugin.writer.txtfilewriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class TxtFileWriter extends Writer {
	public static class Job extends Writer.Job {
		private static final Logger LOG = LoggerFactory
				.getLogger(Job.class);

		private Configuration writerSliceConfig = null;

		@Override
		public void init() {
			LOG.info("init() begin...");
			this.writerSliceConfig = this.getPluginJobConf();
			this.validate();
			LOG.info("init() ok and end...");

		}

		private void validate() {

			this.writerSliceConfig.getNecessaryValue(Key.TRUNCATE,
					TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION);

			String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
					TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION);
			String charset = this.writerSliceConfig.getString(Key.CHARSET,
					Constants.DEFAULT_CHARSET);
			try {
				Charsets.toCharset(charset);
				// 这里用户需要配一个目录
				File dir = new File(path);
				if (dir.isFile()) {
					throw new Exception(String.format(
							"path [%s] is not directory.", path));
				}
				if (!dir.exists()) {
					dir.mkdir();
				}

			} catch (UnsupportedCharsetException uce) {
				throw DataXException.asDataXException(
						TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
						String.format("不支持的编码格式:[%s]", charset), uce);
			} catch (SecurityException se) {
				throw DataXException.asDataXException(
						TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
						String.format("没有权限创建文件路径 : [%s] ", path), se);
			} catch (Exception e) {
				throw DataXException.asDataXException(
						TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
						String.format("运行配置异常: %s", e.getMessage()), e);
			}
		}

		@Override
		public void prepare() {
			LOG.info("prefare() begin...");
			// truncate files
			boolean truncate = this.writerSliceConfig.getBool(Key.TRUNCATE);

			String path = this.writerSliceConfig.getString(Key.PATH);
			if (truncate) {
				File dir = new File(path);
				// warn:需要判断文件是否存在，不存在时，不能删除
				try {
					if (dir.exists()) {
						// warn:不用使用FileUtils.deleteQuietly(dir);
						FileUtils.cleanDirectory(dir);
					}
				} catch (SecurityException se) {
					throw DataXException.asDataXException(
							TxtFileWriterErrorCode.FILE_EXCEPTION,
							String.format("没有权限查看目录 : [%s]", path));
				} catch (IOException e) {
					throw DataXException.asDataXException(
							TxtFileWriterErrorCode.FILE_EXCEPTION,
							String.format("无法清空目录 : [%s]", path));
				}
			}
			LOG.info("prefare() ok and end...");
		}

		@Override
		public void post() {
			LOG.info("post()");
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			LOG.info("split() begin...");
			List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
			String filePrefix = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
					.format(new Date());
			String fileSuffix;

			for (int i = 0; i < mandatoryNumber; i++) {
				Configuration splitedTaskConfig = this.writerSliceConfig
						.clone();

				fileSuffix = UUID.randomUUID().toString().replace('-', '_');
				String fileName = String.format("%s__%s", filePrefix,
						fileSuffix);
				splitedTaskConfig.set(Constants.FILE_NAME, fileName);

				LOG.info(String
						.format("splited write file name:[%s]", fileName));

				writerSplitConfigs.add(splitedTaskConfig);
			}
			LOG.info("split() ok and end...");
			return writerSplitConfigs;
		}

	}

	public static class Task extends Writer.Task {
		private static final Logger LOG = LoggerFactory
				.getLogger(Task.class);

		private Configuration writerSliceConfig;

		private String path;

		private String charset;

		private String fieldDelimiter;

		private String lineDelimiter;

		private String fileName = "0";

		@Override
		public void init() {
			LOG.info("init() begin...");

			this.writerSliceConfig = this.getPluginJobConf();
			this.path = this.writerSliceConfig.getString(Key.PATH);
			this.charset = this.writerSliceConfig.getString(Key.CHARSET,
					Constants.DEFAULT_CHARSET);

			this.fieldDelimiter = this.writerSliceConfig.getString(
					Key.FIELD_DELIMITER, Constants.DEFAULT_FIELD_DELIMITER);

			// TODO 支持行分割符
			this.lineDelimiter = IOUtils.LINE_SEPARATOR;

			this.fileName = this.writerSliceConfig.getString(
					Constants.FILE_NAME, this.fileName);
			LOG.info("init() ok and end...");
		}

		@Override
		public void prepare() {
			LOG.info("prepare()");
		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			LOG.info("startWrite() begin...");
			String fileFullPath = this.buildFilePath();
			LOG.info(String.format("write to file : [%s]", fileFullPath));

			BufferedWriter fileWriter = null;
			try {
				File newFile = new File(fileFullPath);
				newFile.createNewFile();
				fileWriter = new BufferedWriter(new FileWriterWithEncoding(
						newFile, this.charset));

				Record record;
				while ((record = lineReceiver.getFromReader()) != null) {
					fileWriter.write(this.format(record));
				}
			} catch (SecurityException se) {
				throw DataXException.asDataXException(
						TxtFileWriterErrorCode.FILE_EXCEPTION,
						String.format("无法创建文件  : [%s]", this.fileName));
			} catch (IOException ioe) {
				throw DataXException.asDataXException(
						TxtFileWriterErrorCode.FILE_EXCEPTION,
						String.format("无法写文件 : [%s]", this.fileName), ioe);
			} catch (Exception dxe) {
				throw DataXException.asDataXException(
						TxtFileWriterErrorCode.RUNTIME_EXCEPTION,
						String.format("运行时异常 : %s", dxe.getMessage()), dxe);
			} finally {
				IOUtils.closeQuietly(fileWriter);
			}
			LOG.info("startWrite() ok and end...");
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

		private String format(Record record) {
			int recordLength = record.getColumnNumber();
			if (recordLength == 0) {
				return this.lineDelimiter;
			}

			Column column;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < recordLength; i++) {
				column = record.getColumn(i);
				sb.append(column == null ? "" : column.asString());
				if (i != recordLength - 1) {
					sb.append(this.fieldDelimiter);
				}
			}
			sb.append(this.lineDelimiter);

			return sb.toString();
		}

		@Override
		public void post() {
			LOG.info("post()");
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}
	}
}
