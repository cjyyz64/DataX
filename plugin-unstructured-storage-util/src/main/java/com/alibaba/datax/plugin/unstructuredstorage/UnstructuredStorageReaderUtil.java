package com.alibaba.datax.plugin.unstructuredstorage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Scanner;

import org.anarres.lzo.LzoDecompressor1z_safe;
import org.anarres.lzo.LzoInputStream;
import org.anarres.lzo.LzopInputStream;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ar.ArArchiveInputStream;
import org.apache.commons.compress.archivers.arj.ArjArchiveInputStream;
import org.apache.commons.compress.archivers.cpio.CpioArchiveInputStream;
import org.apache.commons.compress.archivers.dump.DumpArchiveInputStream;
import org.apache.commons.compress.archivers.jar.JarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.pack200.Pack200CompressorInputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.csvreader.CsvReader;

public class UnstructuredStorageReaderUtil {
	private static final Logger LOG = LoggerFactory
			.getLogger(UnstructuredStorageReaderUtil.class);

	private UnstructuredStorageReaderUtil() {

	}

	/**
	 * @param inputLine
	 *            输入待分隔字符串
	 * @param delimiter
	 *            字符串分割符
	 * @return 分隔符分隔后的字符串数组，出现异常时返回为null 支持转义，即数据中可包含分隔符
	 * */
	public static String[] splitOneLine(String inputLine, char delimiter) {
		String[] splitedResult = null;
		if (null != inputLine) {
			try {
				CsvReader csvReader = new CsvReader(new StringReader(inputLine));
				csvReader.setDelimiter(delimiter);
				if (csvReader.readRecord()) {
					splitedResult = csvReader.getValues();
				}
			} catch (IOException e) {
				// nothing to do
			}
		}
		return splitedResult;
	}

	/**
	 * 不支持转义
	 * 
	 * @return 分隔符分隔后的字符串数，
	 * */
	public static String[] splitOneLine(String inputLine, String delimiter) {
		String[] sourceLine = StringUtils.split(inputLine, delimiter);
		return sourceLine;
	}

	public static Record transportOneRecord(RecordSender recordSender,
			List<Configuration> columnConfigs, String[] sourceLine,
			String nullFormat, TaskPluginCollector taskPluginCollector) {
		Record record = recordSender.createRecord();
		Column columnGenerated = null;
		// 创建都为String类型column的record（注意，参数 record 要是没有任何字段column设置的纯粹的 record）
		if (null == columnConfigs || columnConfigs.size() == 0) {
			for (String columnValue : sourceLine) {
				if (columnValue.equals(nullFormat)) {
					columnGenerated = new StringColumn(null);
				} else {
					columnGenerated = new StringColumn(columnValue);
				}
				record.addColumn(columnGenerated);
			}
		} else {
			for (Configuration columnConfig : columnConfigs) {
				String columnType = columnConfig.getNecessaryValue(Key.TYPE,
						UnstructuredStorageErrorCode.CONFIG_INVALID_EXCEPTION);
				Integer columnIndex = columnConfig.getInt(Key.INDEX);
				String columnConst = columnConfig.getString(Key.VALUE);

				String columnValue = null;

				try {
					if (null == columnIndex && null == columnConst) {
						throw DataXException.asDataXException(
								UnstructuredStorageErrorCode.NO_INDEX_VALUE,
								"由于您配置了type, 则至少需要配置 index 或 value");
					}

					if (null != columnIndex && null != columnConst) {
						throw DataXException.asDataXException(
								UnstructuredStorageErrorCode.MIXED_INDEX_VALUE,
								"您混合配置了index, value, 每一列同时仅能选择其中一种");
					}

					if (null != columnIndex) {
						if (columnIndex >= sourceLine.length) {
							String message = String.format(
									"您尝试读取的列越界,源文件该行有 [%s] 列,您尝试读取第 [%s] 列",
									sourceLine.length, columnIndex + 1);
							LOG.error(message);
							throw DataXException.asDataXException(
									UnstructuredStorageErrorCode.ILLEGAL_VALUE,
									message);
						}

						columnValue = sourceLine[columnIndex];
					} else {
						columnValue = columnConst;
					}
					Type type = Type.valueOf(columnType.toUpperCase());

					switch (type) {
					case STRING:
						if (columnValue.equals(nullFormat)) {
							columnGenerated = new StringColumn(null);
						} else {
							columnGenerated = new StringColumn(columnValue);
						}
						break;
					case LONG:
						columnGenerated = new LongColumn(columnValue);
						break;
					case DOUBLE:
						columnGenerated = new DoubleColumn(columnValue);
						break;
					case BOOLEAN:
						columnGenerated = new BoolColumn(columnValue);
						break;
					case DATE:
						String formatString = columnConfig
								.getString(Key.FORMAT);
						if (null != formatString) {
							// 用户自己配置的格式转换
							SimpleDateFormat format = new SimpleDateFormat(
									formatString);
							columnGenerated = new DateColumn(
									format.parse(columnValue));
						} else {
							// 框架尝试转换
							columnGenerated = new DateColumn(new StringColumn(
									columnValue).asDate());
						}
						break;
					default:
						String errorMessage = String.format(
								"您配置的列类型暂不支持 : [%s]", columnType);
						LOG.error(errorMessage);
						throw DataXException.asDataXException(
								UnstructuredStorageErrorCode.NOT_SUPPORT_TYPE,
								errorMessage);
					}

					record.addColumn(columnGenerated);

				} catch (IndexOutOfBoundsException ioe) {
					throw DataXException.asDataXException(
							UnstructuredStorageErrorCode.ILLEGAL_VALUE,
							String.format("您配置的索引下标越界 : [%s]", columnIndex));
				} catch (Exception e) {
					// 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
					taskPluginCollector.collectDirtyRecord(record,
							e.getMessage());
				}
			}
		}
		recordSender.sendToWriter(record);
		return record;
	}

	public static void readFromStream(InputStream inputStream,
			String description, Configuration readerSliceConfig,
			RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
		String compress = readerSliceConfig.getString(Key.COMPRESS, null);
		String charset = readerSliceConfig.getString(Key.CHARSET,
				Constant.DEFAULT_CHARSET);
		BufferedReader reader = null;
		// compress logic
		try {
			if (null == compress) {
				reader = new BufferedReader(new InputStreamReader(inputStream,
						charset));
				UnstructuredStorageReaderUtil.doReadFromStream(reader,
						description, readerSliceConfig, recordSender,
						taskPluginCollector);
			} else {
				if ("lzo".equalsIgnoreCase(compress)) {
					LzoInputStream lzoInputStream = new LzoInputStream(
							inputStream, new LzoDecompressor1z_safe());
					reader = new BufferedReader(new InputStreamReader(
							lzoInputStream, charset));
				} else if ("lzop".equalsIgnoreCase(compress)) {
					LzoInputStream lzopInputStream = new LzopInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							lzopInputStream, charset));
				} else if ("gzip".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new GzipCompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, charset));
				} else if ("bzip2".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new BZip2CompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, charset));
				} else if ("lzma".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new LZMACompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, charset));
				} else if ("pack200".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new Pack200CompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, charset));
				} else if ("snappy".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new SnappyCompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, charset));
				} else if ("xz".equalsIgnoreCase(compress)) {
					CompressorInputStream compressorInputStream = new XZCompressorInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							compressorInputStream, charset));
				} else if ("ar".equalsIgnoreCase(compress)) {
					ArArchiveInputStream arArchiveInputStream = new ArArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							arArchiveInputStream, charset));
				} else if ("arj".equalsIgnoreCase(compress)) {
					ArjArchiveInputStream arjArchiveInputStream = new ArjArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							arjArchiveInputStream, charset));
				} else if ("cpio".equalsIgnoreCase(compress)) {
					CpioArchiveInputStream cpioArchiveInputStream = new CpioArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							cpioArchiveInputStream, charset));
				} else if ("dump".equalsIgnoreCase(compress)) {
					DumpArchiveInputStream dumpArchiveInputStream = new DumpArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							dumpArchiveInputStream, charset));
				} else if ("jar".equalsIgnoreCase(compress)) {
					JarArchiveInputStream jarArchiveInputStream = new JarArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							jarArchiveInputStream, charset));
				} else if ("sevenz".equalsIgnoreCase(compress)) {
					// TODO
				} else if ("tar".equalsIgnoreCase(compress)) {
					TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							tarArchiveInputStream, charset));
				} else if ("zip".equalsIgnoreCase(compress)) {
					ZipArchiveInputStream zipArchiveInputStream = new ZipArchiveInputStream(
							inputStream);
					reader = new BufferedReader(new InputStreamReader(
							zipArchiveInputStream, charset));
				} else {
					throw DataXException
							.asDataXException(
									UnstructuredStorageErrorCode.ILLEGAL_VALUE,
									String.format(
											"仅支持 lzo, lzop, gzip, bzip2, lzma, pack200, snappy, xz 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]",
											compress));
				}

				UnstructuredStorageReaderUtil.doReadFromStream(reader,
						description, readerSliceConfig, recordSender,
						taskPluginCollector);
			}
		} catch (UnsupportedEncodingException uee) {
			throw DataXException.asDataXException(
					UnstructuredStorageErrorCode.OPEN_FILE_WITH_CHARSET_ERROR,
					String.format("不支持的编码格式 : [%]", charset), uee);
		} catch (NullPointerException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageErrorCode.RUNTIME_EXCEPTION,
					"运行时错误, 请联系我们", e);
		} catch (ArchiveException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageErrorCode.READ_FILE_IO_ERROR,
					String.format("压缩文件流读取错误 : [%]", description), e);
		} catch (IOException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageErrorCode.READ_FILE_IO_ERROR,
					String.format("流读取错误 : [%]", description), e);
		} finally {
			IOUtils.closeQuietly(reader);
		}

	}

	private static void doReadFromStream(BufferedReader reader,
			String description, Configuration readerSliceConfig,
			RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
		List<Configuration> column = readerSliceConfig
				.getListConfiguration(Key.COLUMN);
		String charset = readerSliceConfig.getString(Key.CHARSET,
				Constant.DEFAULT_CHARSET);
		char fieldDelimiter = readerSliceConfig.getChar(Key.FIELD_DELIMITER,
				Constant.DEFAULT_FIELD_DELIMITER);
		Boolean skipHeader = readerSliceConfig.getBool(Key.SKIP_HEADER,
				Constant.DEFAULT_SKIP_HEADER);
		String nullFormat = readerSliceConfig.getString(Key.NULL_FORMAT,
				Constant.DEFAULT_NULL_FORMAT);

		// every line logic
		try {
			String fetchLine = null;
			while ((fetchLine = reader.readLine()) != null) {
				if (skipHeader) {
					continue;
				}
				String[] splitedStrs = UnstructuredStorageReaderUtil
						.splitOneLine(fetchLine, fieldDelimiter);
				UnstructuredStorageReaderUtil.transportOneRecord(recordSender,
						column, splitedStrs, nullFormat, taskPluginCollector);
			}
		} catch (UnsupportedEncodingException uee) {
			throw DataXException.asDataXException(
					UnstructuredStorageErrorCode.OPEN_FILE_WITH_CHARSET_ERROR,
					String.format("不支持的编码格式 : [%]", charset), uee);
		} catch (FileNotFoundException fnfe) {
			throw DataXException.asDataXException(
					UnstructuredStorageErrorCode.FILE_NOT_EXISTS,
					String.format("无法找到文件 : [%s]", description), fnfe);
		} catch (IOException ioe) {
			throw DataXException.asDataXException(
					UnstructuredStorageErrorCode.READ_FILE_IO_ERROR,
					String.format("读取文件错误 : [%s]", description), ioe);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					UnstructuredStorageErrorCode.RUNTIME_EXCEPTION,
					String.format("运行时异常 : %s", e.getMessage()), e);
		} finally {
			IOUtils.closeQuietly(reader);
		}
	}

	private enum Type {
		STRING, LONG, BOOLEAN, DOUBLE, DATE, ;
	}

	public static void main(String args[]) {
		while (true) {
			@SuppressWarnings("resource")
			Scanner sc = new Scanner(System.in);
			String inputString = sc.nextLine();
			String delemiter = sc.nextLine();
			if (delemiter.length() == 0) {
				break;
			}
			if (!inputString.equals("exit")) {
				String[] result = UnstructuredStorageReaderUtil.splitOneLine(
						inputString, delemiter.charAt(0));
				for (String str : result) {
					System.out.print(str + " ");
				}
				System.out.println();
			} else {
				break;
			}
		}
	}
}
