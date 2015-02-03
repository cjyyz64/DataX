package com.alibaba.datax.plugin.unstructuredstorage.writer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;

import org.anarres.lzo.LzoCompressor1x_1;
import org.anarres.lzo.LzoOutputStream;
import org.anarres.lzo.LzopOutputStream;
import org.apache.commons.compress.archivers.ar.ArArchiveOutputStream;
import org.apache.commons.compress.archivers.cpio.CpioArchiveOutputStream;
import org.apache.commons.compress.archivers.jar.JarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.pack200.Pack200CompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.io.IOUtils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;

public class UnstructuredStorageWriterUtil {
	private UnstructuredStorageWriterUtil() {

	}

	public static void writeToStream(RecordReceiver lineReceiver,
			OutputStream outputStream, Configuration config, String context,
			TaskPluginCollector taskPluginCollector) {
		String encoding = config.getString(Key.ENCODING,
				Constant.DEFAULT_CHARSET);
		String compress = config.getString(Key.COMPRESS);

		BufferedWriter writer = null;
		// compress logic
		try {
			if (null == compress) {
				writer = new BufferedWriter(new OutputStreamWriter(
						outputStream, encoding));
			} else {
				// TODO compress
				if ("lzo".equalsIgnoreCase(compress)) {

					LzoOutputStream lzoOutputStream = new LzoOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							lzoOutputStream, encoding));
				} else if ("lzop".equalsIgnoreCase(compress)) {
					LzoOutputStream lzopOutputStream = new LzopOutputStream(
							outputStream, new LzoCompressor1x_1());
					writer = new BufferedWriter(new OutputStreamWriter(
							lzopOutputStream, encoding));
				} else if ("gzip".equalsIgnoreCase(compress)) {
					CompressorOutputStream compressorOutputStream = new GzipCompressorOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							compressorOutputStream, encoding));
				} else if ("bzip2".equalsIgnoreCase(compress)) {
					CompressorOutputStream compressorOutputStream = new BZip2CompressorOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							compressorOutputStream, encoding));
				} else if ("pack200".equalsIgnoreCase(compress)) {
					CompressorOutputStream compressorOutputStream = new Pack200CompressorOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							compressorOutputStream, encoding));
				} else if ("xz".equalsIgnoreCase(compress)) {
					CompressorOutputStream compressorOutputStream = new XZCompressorOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							compressorOutputStream, encoding));
				} else if ("ar".equalsIgnoreCase(compress)) {
					ArArchiveOutputStream arArchiveOutputStream = new ArArchiveOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							arArchiveOutputStream, encoding));
				} else if ("cpio".equalsIgnoreCase(compress)) {
					CpioArchiveOutputStream cpioArchiveOutputStream = new CpioArchiveOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							cpioArchiveOutputStream, encoding));
				} else if ("jar".equalsIgnoreCase(compress)) {
					JarArchiveOutputStream jarArchiveOutputStream = new JarArchiveOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							jarArchiveOutputStream, encoding));
				} else if ("tar".equalsIgnoreCase(compress)) {
					TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							tarArchiveOutputStream, encoding));
				} else if ("zip".equalsIgnoreCase(compress)) {
					ZipArchiveOutputStream zipArchiveOutputStream = new ZipArchiveOutputStream(
							outputStream);
					writer = new BufferedWriter(new OutputStreamWriter(
							zipArchiveOutputStream, encoding));
				} else {
					throw DataXException
							.asDataXException(
									UnstructuredStorageWriterErrorCode.ILLEGAL_VALUE,
									String.format(
											"仅支持 lzo, lzop, gzip, bzip2, pack200, xz, ar, cpio, jar, tar, zip 文件压缩格式 , 不支持您配置的文件压缩格式: [%s]",
											compress));
				}
			}
			UnstructuredStorageWriterUtil.doWriteToStream(lineReceiver, writer,
					context, config, taskPluginCollector);
		} catch (UnsupportedEncodingException uee) {
			throw DataXException
					.asDataXException(
							UnstructuredStorageWriterErrorCode.Write_FILE_WITH_CHARSET_ERROR,
							String.format("不支持的编码格式 : [%]", encoding), uee);
		} catch (NullPointerException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageWriterErrorCode.RUNTIME_EXCEPTION,
					"运行时错误, 请联系我们", e);
		} catch (IOException e) {
			throw DataXException.asDataXException(
					UnstructuredStorageWriterErrorCode.Write_FILE_IO_ERROR,
					String.format("流写入错误 : [%]", context), e);
		} finally {
			IOUtils.closeQuietly(writer);
		}
	}

	private static void doWriteToStream(RecordReceiver lineReceiver,
			BufferedWriter writer, String contex, Configuration config,
			TaskPluginCollector taskPluginCollector) throws IOException {

		String nullFormat = config.getString(Key.NULL_FORMAT);

		String dateFormat = config.getString(Key.FORMAT);
		// warn: fieldDelimiter could be '' for no fieldDelimiter
		char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER,
				Constant.DEFAULT_FIELD_DELIMITER);

		Record record = null;
		while ((record = lineReceiver.getFromReader()) != null) {
			String line = UnstructuredStorageWriterUtil.transportOneRecord(
					record, nullFormat, dateFormat, fieldDelimiter,
					taskPluginCollector);
			writer.write(line);
		}
	}

	private static String transportOneRecord(Record record, String nullFormat,
			String dateFormat, char fieldDelimiter,
			TaskPluginCollector taskPluginCollector) {
		StringBuilder sb = new StringBuilder();
		int recordLength = record.getColumnNumber();
		if (0 != recordLength) {
			Column column;
			for (int i = 0; i < recordLength; i++) {
				column = record.getColumn(i);
				if (null != column.getRawData()) {
					boolean isDateColumn = column instanceof DateColumn;
					if (!isDateColumn) {
						sb.append(column.asString());
					} else {
						if (null != dateFormat) {
							try {
								SimpleDateFormat dateParse = new SimpleDateFormat(
										dateFormat);
								sb.append(dateParse.format(column.asDate()));
							} catch (Exception e) {
								// warn: 此处认为似乎脏数据
								String message = String.format(
										"使用您配置的格式 [%s] 转换 [%s] 错误.",
										dateFormat, column.asString());
								taskPluginCollector.collectDirtyRecord(record,
										message);
							}
						} else {
							sb.append(column.asString());
						}
					}
				} else {
					// warn: it's all ok if nullFormat is null
					sb.append(nullFormat);
				}
				if (i != recordLength - 1) {
					sb.append(fieldDelimiter);
				}
			}
		}
		sb.append(IOUtils.LINE_SEPARATOR);
		return sb.toString();
	}
}
