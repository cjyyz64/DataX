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

		private Configuration readerSliceConfig = null;
		private String path = null;
		private List<String> sourceFiles;

		@Override
		public void init() {
			LOG.info("init()");
			this.readerSliceConfig = this.getPluginJobConf();
			this.validate();
		}

		// TODO validate column
		private void validate() {
			path = this.readerSliceConfig.getNecessaryValue(Key.PATH,
					TxtFileReaderErrorCode.CONFIG_INVALID_EXCEPTION);
			String charset = this.readerSliceConfig.getString(Key.CHARSET,
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

		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();

			List<List<String>> splitedSourceFiles = this.splitSourceFiles(
					sourceFiles, adviceNumber);
			for (List<String> files : splitedSourceFiles) {
				Configuration splitedConfig = this.readerSliceConfig.clone();
				splitedConfig.set(Constants.SOURCE_FILES, files);
				readerSplitConfigs.add(splitedConfig);
			}
			return readerSplitConfigs;
		}

		// TODO validate the path?
		// path must be a absolute path
		private List<String> buildSourceTargets() {
			// 获取路径前缀，无 * ?
			int firstMark = this.path.indexOf('*');
			int firstQuestionMark = this.path.indexOf('?');
			firstMark = firstMark < firstQuestionMark ? firstMark
					: firstQuestionMark;
			int lastDirSeparator = this.path.substring(0, firstMark)
					.lastIndexOf(IOUtils.DIR_SEPARATOR);
			String parentDirectory = this.path.substring(0,
					lastDirSeparator + 1);
			return this.buildSourceTargets(parentDirectory);
		}

		private List<String> buildSourceTargets(String sourcePath) {
			List<String> sourceFiles = new ArrayList<String>();
			buildSourceTargets(sourceFiles, sourcePath);
			return sourceFiles;
		}

		private void buildSourceTargets(List<String> result, String sourcePath) {
			File directory = new File(sourcePath);
			if (!directory.isDirectory()) {
				if (this.isTargetFile(directory.getAbsolutePath())) {
					result.add(sourcePath);
				}
			} else {
				for (String subFileNames : directory.list()) {
					buildSourceTargets(result, subFileNames);
				}
			}
		}

		// TODO 正则过滤
		private boolean isTargetFile(String absoluteFilePath) {
			return true;
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

		@Override
		public void startRead(RecordSender recordSender) {
			for (String fileName : this.sourceFiles) {
				this.readerFromFile(fileName, recordSender);
				recordSender.flush();
			}
		}

		// TODO 优化异常相关
		// TODO readLine lineDelimiter 字符 or 字符串
		private void readerFromFile(String fileName, RecordSender recordSender) {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(
						new FileInputStream(fileName), this.charset));
				String fetchLine = null;
				Record record = null;
				while ((fetchLine = reader.readLine()) != null) {
					record = recordSender.createRecord();
					String[] sourceLine = fetchLine.split(this.fieldDelimiter);
					Column columnGenerated;
					// 未配置column 全为String
					if (this.column == null) {
						for (String columnValue : sourceLine) {
							columnGenerated = new StringColumn(columnValue);
							record.addColumn(columnGenerated);
						}
					} else {
						// 根据用户配置column生成
						for (Configuration conf : this.column) {
							columnGenerated = this.generateColumn(conf,
									sourceLine);
						}
					}
					recordSender.sendToWriter(record);
				}
			} catch (UnsupportedEncodingException uee) {
				throw new DataXException(
						TxtFileReaderErrorCode.FILE_EXCEPTION,
						String.format("could not use charset [%]", this.charset),
						uee);
			} catch (FileNotFoundException fnfe) {
				throw new DataXException(TxtFileReaderErrorCode.FILE_EXCEPTION,
						String.format("could not find file [%s]", fileName),
						fnfe);
			} catch (IOException ioe) {
				throw new DataXException(TxtFileReaderErrorCode.FILE_EXCEPTION,
						String.format("read file error [%s]", fileName), ioe);
			} catch (Exception e) {
				throw new DataXException(
						TxtFileReaderErrorCode.RUNTIME_EXCEPTION,
						e.getMessage(), e);
			} finally {
				IOUtils.closeQuietly(reader);
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
							"not support column type [%s]", columnType);
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
