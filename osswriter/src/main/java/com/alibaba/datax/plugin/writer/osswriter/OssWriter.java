package com.alibaba.datax.plugin.writer.osswriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.writer.osswriter.util.OssUtil;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

/**
 * Created by haiwei.luo on 15-02-09.
 */
public class OssWriter extends Writer {
	public static class Job extends Writer.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);

		private Configuration writerSliceConfig = null;
		private OSSClient ossClient = null;

		@Override
		public void init() {
			this.writerSliceConfig = this.getPluginJobConf();
			this.validateParameter();
			this.ossClient = OssUtil.initOssClient(this.writerSliceConfig);
		}

		private void validateParameter() {
			this.writerSliceConfig.getNecessaryValue(Key.ENDPOINT,
					OssWriterErrorCode.REQUIRED_VALUE);
			this.writerSliceConfig.getNecessaryValue(Key.ACCESSID,
					OssWriterErrorCode.REQUIRED_VALUE);
			this.writerSliceConfig.getNecessaryValue(Key.ACCESSKEY,
					OssWriterErrorCode.REQUIRED_VALUE);
			this.writerSliceConfig.getNecessaryValue(Key.BUCKET,
					OssWriterErrorCode.REQUIRED_VALUE);
			this.writerSliceConfig.getNecessaryValue(Key.OBJECT,
					OssWriterErrorCode.REQUIRED_VALUE);
			// warn: do not support compress!!
			String compress = this.writerSliceConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.COMPRESS);
			if (StringUtils.isNotBlank(compress)) {
				this.writerSliceConfig
						.remove(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.COMPRESS);
				LOG.warn("OSS写暂时不支持压缩, 该配置项不起效用.");

			}
			UnstructuredStorageWriterUtil
					.validateParameter(this.writerSliceConfig);

		}

		@Override
		public void prepare() {
			LOG.info("begin do prepare...");
			String bucket = this.writerSliceConfig.getString(Key.BUCKET);
			String object = this.writerSliceConfig.getString(Key.OBJECT);
			String writeMode = this.writerSliceConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
			// warn: bucket is not exists, create it
			try {
				if (!this.ossClient.doesBucketExist(bucket)) {
					this.ossClient.createBucket(bucket);
				}
				LOG.info(String.format("access control details [%s].",
						this.ossClient.getBucketAcl(bucket).toString()));

				// truncate option handler
				if ("truncate".equals(writeMode)) {
					LOG.info(String
							.format("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的Object",
									bucket, object));
					ObjectListing listing = this.ossClient.listObjects(bucket,
							object);
					for (OSSObjectSummary objectSummary : listing
							.getObjectSummaries()) {
						LOG.info(String.format("delete oss object [%s].",
								objectSummary.getKey()));
						this.ossClient.deleteObject(bucket,
								objectSummary.getKey());
					}
				} else if ("append".equals(writeMode)) {
					LOG.info(String
							.format("由于您配置了writeMode append, 写入前不做清理工作, 数据写入Bucket [%s] 下, 写入相应Object的前缀为  [%s]",
									bucket, object));
				} else if ("error".equals(writeMode)) {
					LOG.info(String
							.format("由于您配置了writeMode error, 开始检查Bucket [%s] 下面以 [%s] 命名开头的Object",
									bucket, object));
					ObjectListing listing = this.ossClient.listObjects(bucket,
							object);
					if (0 < listing.getObjectSummaries().size()) {
						throw DataXException
								.asDataXException(
										OssWriterErrorCode.ILLEGAL_VALUE,
										String.format(
												"您配置的Bucket: [%s] 下面存在其Object有前缀 [%s].",
												bucket, object));
					}
				}
			} catch (OSSException e) {
				throw DataXException.asDataXException(
						OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
			} catch (ClientException e) {
				throw DataXException.asDataXException(
						OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
			}
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
			String object = this.writerSliceConfig.getString(Key.OBJECT);
			String bucket = this.writerSliceConfig.getString(Key.BUCKET);

			Set<String> allObjects = new HashSet<String>();
			try {
				List<OSSObjectSummary> ossObjectlisting = this.ossClient
						.listObjects(bucket).getObjectSummaries();
				for (OSSObjectSummary objectSummary : ossObjectlisting) {
					allObjects.add(objectSummary.getKey());
				}
			} catch (OSSException e) {
				throw DataXException.asDataXException(
						OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
			} catch (ClientException e) {
				throw DataXException.asDataXException(
						OssWriterErrorCode.OSS_COMM_ERROR, e.getMessage());
			}

			String objectSuffix;
			for (int i = 0; i < mandatoryNumber; i++) {
				// handle same object name
				Configuration splitedTaskConfig = this.writerSliceConfig
						.clone();

				String fullObjectName = null;
				objectSuffix = UUID.randomUUID().toString().replace('-', '_');
				fullObjectName = String.format("%s__%s", object, objectSuffix);
				while (allObjects.contains(fullObjectName)) {
					objectSuffix = UUID.randomUUID().toString()
							.replace('-', '_');
					fullObjectName = String.format("%s__%s", object,
							objectSuffix);
				}
				allObjects.add(fullObjectName);

				splitedTaskConfig.set(Key.OBJECT, fullObjectName);

				LOG.info(String.format("splited write object name:[%s]",
						fullObjectName));

				writerSplitConfigs.add(splitedTaskConfig);
			}
			LOG.info("end do split.");
			return writerSplitConfigs;
		}
	}

	public static class Task extends Writer.Task {
		private static final Logger LOG = LoggerFactory.getLogger(Task.class);

		private OSSClient ossClient;
		private Configuration writerSliceConfig;
		private String bucket;
		private String object;
		private String nullFormat;
		private String encoding;
		private char fieldDelimiter;
		private String format;

		@Override
		public void init() {
			this.writerSliceConfig = this.getPluginJobConf();
			this.ossClient = OssUtil.initOssClient(this.writerSliceConfig);
			this.bucket = this.writerSliceConfig.getString(Key.BUCKET);
			this.object = this.writerSliceConfig.getString(Key.OBJECT);
			this.nullFormat = this.writerSliceConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.NULL_FORMAT);
			this.format = this.writerSliceConfig
					.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FORMAT);
			this.encoding = this.writerSliceConfig
					.getString(
							com.alibaba.datax.plugin.unstructuredstorage.writer.Key.ENCODING,
							com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_ENCODING);
			this.fieldDelimiter = this.writerSliceConfig
					.getChar(
							com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FIELD_DELIMITER,
							com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_FIELD_DELIMITER);
		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			LOG.info("begin do write...");
			InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
					this.bucket, this.object);
			InitiateMultipartUploadResult initiateMultipartUploadResult = this.ossClient
					.initiateMultipartUpload(initiateMultipartUploadRequest);
			LOG.info(String
					.format("write to bucket: [%s] object: [%s] with oss uploadId: [%s]",
							this.bucket, this.object,
							initiateMultipartUploadResult.getUploadId()));
			// 设置每块字符串长度
			final int partSize = 1024 * 1024 * 1;
			int partNumber = 1;
			StringBuilder sb = new StringBuilder();
			Record record = null;
			List<PartETag> partETags = new ArrayList<PartETag>();
			try {
				while ((record = lineReceiver.getFromReader()) != null) {
					String line = UnstructuredStorageWriterUtil
							.transportOneRecord(record, nullFormat, format,
									fieldDelimiter,
									this.getTaskPluginCollector());
					sb.append(line);

					if (sb.length() >= partSize) {
						this.uploadOnePart(sb, partNumber,
								initiateMultipartUploadResult, partETags);
						partNumber++;
						sb = new StringBuilder();
					}
				}

				if (0 < sb.length()) {
					this.uploadOnePart(sb, partNumber,
							initiateMultipartUploadResult, partETags);
				}
				CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
						this.bucket, this.object,
						initiateMultipartUploadResult.getUploadId(), partETags);
				CompleteMultipartUploadResult completeMultipartUploadResult = this.ossClient
						.completeMultipartUpload(completeMultipartUploadRequest);
				LOG.info(String.format("final object etag is:[%s]",
						completeMultipartUploadResult.getETag()));

			} catch (UnsupportedEncodingException e) {
				throw DataXException.asDataXException(
						OssWriterErrorCode.ILLEGAL_VALUE,
						String.format("不支持您配置的编码格式:[%s]", encoding));
			} catch (OSSException e) {
				throw DataXException.asDataXException(
						OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
			} catch (ClientException e) {
				throw DataXException.asDataXException(
						OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
			} catch (IOException e) {
				throw DataXException.asDataXException(
						OssWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
			}
			LOG.info("end do write");
		}

		private void uploadOnePart(StringBuilder sb, int partNumber,
				InitiateMultipartUploadResult initiateMultipartUploadResult,
				List<PartETag> partETags) throws IOException,
				UnsupportedEncodingException {
			byte[] byteArray = sb.toString().getBytes(this.encoding);
			InputStream inputStream = new ByteArrayInputStream(byteArray);
			// 创建UploadPartRequest，上传分块
			UploadPartRequest uploadPartRequest = new UploadPartRequest();
			uploadPartRequest.setBucketName(this.bucket);
			uploadPartRequest.setKey(this.object);
			uploadPartRequest.setUploadId(initiateMultipartUploadResult
					.getUploadId());
			uploadPartRequest.setInputStream(inputStream);
			uploadPartRequest.setPartSize(byteArray.length);
			uploadPartRequest.setPartNumber(partNumber);
			UploadPartResult uploadPartResult = this.ossClient
					.uploadPart(uploadPartRequest);
			partETags.add(uploadPartResult.getPartETag());
			LOG.info(String.format(
					"upload part [%s] size [%s] Byte has been completed.",
					partNumber, byteArray.length));
			inputStream.close();
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
	}
}
