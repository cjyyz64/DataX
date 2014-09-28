package com.alibaba.datax.plugin.reader.txtfilereader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public class TxtFileReader extends Reader {
	public static class Master extends Reader.Master {
		private static final Logger LOG = LoggerFactory
				.getLogger(TxtFileReader.Master.class);

		private Configuration readerOriginConfig = null;
		private String path = null;
		private List<String> sourceFiles;
		private Pattern pattern;
		private boolean isRegexPath;

		@Override
		public void init() {
			LOG.info("init() begin...");
			this.readerOriginConfig = this.getPluginJobConf();
			this.validate();
			LOG.info("init() ok and end...");
		}

		// TODO 文件 权限
		// TODO validate column
		private void validate() {
			path = this.readerOriginConfig.getNecessaryValue(Key.PATH,
					TxtFileReaderErrorCode.CONFIG_INVALID_EXCEPTION);
			String charset = this.readerOriginConfig.getString(Key.CHARSET,
					Constants.DEFAULT_CHARSET);
			try {
				Charsets.toCharset(charset);
			} catch (UnsupportedCharsetException uce) {
				throw new DataXException(
						TxtFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
						uce.getMessage(), uce);
			} catch (Exception e) {
				throw new DataXException(
						TxtFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
						e.getMessage(), e);
			}

		}

		@Override
		public void prepare() {
			LOG.info("prepare()");
			// warn:make sure this regex string
			String regexString = this.path.replace("*", ".*")
					.replace("?", ".?");
			pattern = Pattern.compile(regexString);
			this.sourceFiles = this.buildSourceTargets();
		}

		@Override
		public void post() {
			LOG.info("post()");
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}

		// TODO 如果源目录为空，这时候出错
		@Override
		public List<Configuration> split(int adviceNumber) {
			LOG.info("split() begin...");
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

			List<List<String>> splitedSourceFiles = this.splitSourceFiles(
					this.sourceFiles, adviceNumber);
			for (List<String> files : splitedSourceFiles) {
				Configuration splitedConfig = this.readerOriginConfig.clone();
				splitedConfig.set(Constants.SOURCE_FILES, files);
				readerSplitConfigs.add(splitedConfig);
			}
			LOG.info("split() ok and end...");
			return readerSplitConfigs;
		}

		// TODO validate the path?
		// path must be a absolute path
		private List<String> buildSourceTargets() {
			// 获取路径前缀，无 * ?
			int endMark;
			for (endMark = 0; endMark < this.path.length(); endMark++) {
				if ('*' != this.path.charAt(endMark)
						&& '?' != this.path.charAt(endMark)) {
				} else {
					this.isRegexPath = true;
					break;
				}
			}

			String parentDirectory;
			if (this.isRegexPath) {
				int lastDirSeparator = this.path.substring(0, endMark)
						.lastIndexOf(IOUtils.DIR_SEPARATOR);
				parentDirectory = this.path.substring(0, lastDirSeparator + 1);
			} else {
				parentDirectory = this.path;
			}

			return this.buildSourceTargets(parentDirectory);
		}

		private List<String> buildSourceTargets(String parentDirectory) {
			// 检测目录是否存在，错误情况更明确
			try {
				File dir = new File(parentDirectory);
				boolean isExists = dir.exists();
				if (!isExists) {
					String message = String.format(
							"the directory does not exist : [%s]",
							parentDirectory);
					LOG.error(message);
					throw new DataXException(
							TxtFileReaderErrorCode.FILE_EXCEPTION, message);
				}
			} catch (SecurityException se) {
				String message = String.format(
						"do not have permission to : [%s]", parentDirectory);
				LOG.error(message);
				throw new DataXException(
						TxtFileReaderErrorCode.SECURITY_EXCEPTION, message);
			}

			List<String> sourceFiles = new ArrayList<String>();
			buildSourceTargets(sourceFiles, parentDirectory);
			return sourceFiles;
		}

		private void buildSourceTargets(List<String> result,
				String parentDirectory) {
			File directory = new File(parentDirectory);
			// 是文件
			if (!directory.isDirectory()) {
				if (this.isTargetFile(directory.getAbsolutePath())) {
					result.add(parentDirectory);
					LOG.info(String.format(
							"add file [%s] as a candidate to read",
							parentDirectory));
					// 文件数量限制
					if (result.size() > Constants.MAX_FILE_READ) {
						throw new DataXException(
								TxtFileReaderErrorCode.RUNTIME_EXCEPTION,
								String.format("too much files to read > [%d]",
										Constants.MAX_FILE_READ));
					}
				}
			} else {
				// 是目录
				try {
					// warn:对于没有权限的目录,listFiles 返回null，而不是抛出SecurityException
					File[] files = directory.listFiles();
					if (null != files) {
						for (File subFileNames : files) {
							buildSourceTargets(result,
									subFileNames.getAbsolutePath());
						}
					} else {
						// TODO 对于没有权限的文件，是直接throw DataXException 还是仅仅LOG.warn,
						// =》如何区分无用的隐藏文件（无权限），和用户指定的文件（无权限）
						LOG.warn(String.format("unable list files for : [%s]",
								directory));
					}

				} catch (SecurityException e) {
					LOG.warn(e.getMessage());
				}
			}
		}

		// 正则过滤
		private boolean isTargetFile(String absoluteFilePath) {
			LOG.info(absoluteFilePath);
			if (this.isRegexPath) {
				return this.pattern.matcher(absoluteFilePath).matches();
			} else {
				return true;
			}

		}

		private <T> List<List<T>> splitSourceFiles(final List<T> sourceList,
				int adviceNumber) {
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

	public static class Slave extends Reader.Slave {
		private static Logger LOG = LoggerFactory
				.getLogger(TxtFileReader.Slave.class);

		private Configuration readerSliceConfig;
		private List<Configuration> column;
		private String charset;
		private String fieldDelimiter;
		private List<String> sourceFiles;

		@Override
		public void init() {
			LOG.info("init()");

			this.readerSliceConfig = this.getPluginJobConf();

			this.column = this.readerSliceConfig
					.getListConfiguration(Key.COLUMN);

			charset = this.readerSliceConfig.getString(Key.CHARSET,
					Constants.DEFAULT_CHARSET);
			fieldDelimiter = this.readerSliceConfig.getString(
					Key.FIELD_DELIMITER, Constants.DEFAULT_FIELD_DELIMITER);
			this.sourceFiles = this.readerSliceConfig.getList(
					Constants.SOURCE_FILES, String.class);

		}

		@Override
		public void prepare() {
			LOG.info("prepare()");
		}

		@Override
		public void post() {
			LOG.info("post()");
		}

		@Override
		public void destroy() {
			LOG.info("destroy()");
		}

		// TODO sock 文件无法read
		// TODO check print exception stack
		@Override
		public void startRead(RecordSender recordSender) {
			LOG.info("start startRead()");
			for (String fileName : this.sourceFiles) {
				LOG.info(String.format("reading file : [%s]", fileName));
				
				try {
					this.readFromFile(fileName, recordSender);
					recordSender.flush();
				} catch (Exception e) {
					// 一个文件失败，不能影响所有文件的传输?
					LOG.warn(String.format("could not read file : [%s]",
							fileName));
				}
			}
			LOG.info("end startRead()");
		}

		// TODO readLine lineDelimiter 字符 or 字符串
		private void readFromFile(String fileName, RecordSender recordSender) {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(
						new FileInputStream(fileName), this.charset));
				String fetchLine = null;
				while ((fetchLine = reader.readLine()) != null) {
					String[] sourceLine = fetchLine.split(this.fieldDelimiter);
					// 未配置column 全为String
					if (null == this.column || 0 == this.column.size()) {
						this.generateAndSendStringRecord(recordSender,
								sourceLine);
					} else {
						// 根据用户配置column生成
						this.generateAndSendConfigRecord(recordSender,
								sourceLine);
					}
				}
			} catch (UnsupportedEncodingException uee) {
				throw new DataXException(TxtFileReaderErrorCode.FILE_EXCEPTION,
						String.format("could not use charset : [%]",
								this.charset), uee);
			} catch (FileNotFoundException fnfe) {
				throw new DataXException(TxtFileReaderErrorCode.FILE_EXCEPTION,
						String.format("could not find file : [%s]", fileName),
						fnfe);
			} catch (IOException ioe) {
				throw new DataXException(TxtFileReaderErrorCode.FILE_EXCEPTION,
						String.format("read file error : [%s]", fileName), ioe);
			} catch (Exception e) {
				throw new DataXException(
						TxtFileReaderErrorCode.RUNTIME_EXCEPTION,
						e.getMessage(), e);
			} finally {
				IOUtils.closeQuietly(reader);
			}
		}

		// 创建都为String类型column的record
		private Record generateAndSendStringRecord(RecordSender recordSender,
				String[] sourceLine) {

			Record record;
			Column columnGenerated;
			record = recordSender.createRecord();

			for (String columnValue : sourceLine) {
				columnGenerated = new StringColumn(columnValue);
				record.addColumn(columnGenerated);
			}

			recordSender.sendToWriter(record);
			return record;
		}

		private Record generateAndSendConfigRecord(RecordSender recordSender,
				String[] sourceLine) {
			// 根据用户配置column生成
			Record record;
			Column columnGenerated;
			try {
				record = recordSender.createRecord();
				for (Configuration conf : this.column) {
					columnGenerated = this.generateColumn(conf, sourceLine);
					record.addColumn(columnGenerated);
				}

				recordSender.sendToWriter(record);
				return record;

			} catch (DataXException dxe) {
				// 脏数据处理,已经调用sendToWriter
				record = this.generateAndSendStringRecord(recordSender,
						sourceLine);

				String dirtyDataMessage = String.format(
						"got a dirty data : [%s]", record);
				this.getSlavePluginCollector().collectDirtyRecord(record,
						dirtyDataMessage);
				LOG.warn(dirtyDataMessage);

				return record;

			} catch (Exception e) {
				throw new DataXException(
						TxtFileReaderErrorCode.RUNTIME_EXCEPTION,
						e.getMessage());
			}
		}

		private Column generateColumn(Configuration columnConfig,
				String[] sourceLine) {
			String columnType = columnConfig.getNecessaryValue(Key.TYPE,
					TxtFileReaderErrorCode.CONFIG_INVALID_EXCEPTION);
			Integer columnIndex = columnConfig.getInt(Key.INDEX);
			String columnConst = columnConfig.getString(Key.CONST);

			String columnValue = null;

			try {
				if (null != columnIndex) {
					columnValue = sourceLine[columnIndex];
				} else {
					columnValue = columnConst;
				}

				if ("string".equalsIgnoreCase(columnType)
						|| "char".equalsIgnoreCase(columnType)) {
					return new StringColumn(columnValue);
				} else if ("long".equalsIgnoreCase(columnType)) {
					return new NumberColumn(Long.parseLong(columnValue));
				} else if ("int".equalsIgnoreCase(columnType)) {
					return new NumberColumn(Integer.parseInt(columnValue));
				} else if ("float".equalsIgnoreCase(columnType)) {
					return new NumberColumn(Float.parseFloat(columnValue));
				} else if ("double".equalsIgnoreCase(columnType)) {
					return new NumberColumn(Double.parseDouble(columnValue));
				} else if ("bool".equalsIgnoreCase(columnType)) {
					return new BoolColumn(Boolean.parseBoolean(columnValue));
				} else if ("date".equalsIgnoreCase(columnType)) {
					return new DateColumn(Long.parseLong(columnValue));
				} else if ("byte".equalsIgnoreCase(columnType)) {
					return new BytesColumn(((String) columnValue).getBytes());
				} else {
					String errorMessage = String.format(
							"not support column type :[%s]", columnType);
					LOG.error(errorMessage);
					throw new DataXException(
							TxtFileReaderErrorCode.NOT_SUPPORT_TYPE,
							errorMessage);
				}
			} catch (IndexOutOfBoundsException ioe) {
				throw new DataXException(
						TxtFileReaderErrorCode.CONFIG_INVALID_EXCEPTION,
						String.format("index [%s] is out of bounds",
								columnIndex));
			} catch (NumberFormatException nfe) {
				throw new DataXException(
						TxtFileReaderErrorCode.CAST_VALUE_TYPE_ERROR,
						nfe.getMessage(), nfe);
			}
		}
	}

}
