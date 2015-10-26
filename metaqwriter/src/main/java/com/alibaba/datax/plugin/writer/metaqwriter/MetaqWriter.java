package com.alibaba.datax.plugin.writer.metaqwriter;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.taobao.metaq.client.MetaProducer;

/**
 * Created by zeqi.cw on 2015/10/20.
 */
public class MetaqWriter extends Writer{

    private static final Logger logger = LoggerFactory.getLogger(MetaqWriter.class);

    public static class Job extends Writer.Job {

        private Configuration originalConfig = null;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
        	 List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
             for (int i = 0; i < mandatoryNumber; i++) {
                 writerSplitConfigs.add(this.originalConfig.clone());
             }
             return writerSplitConfigs;
        }

		@Override
        public void init() {
        	 this.originalConfig = super.getPluginJobConf();
        	 validateParameter();
        }

        @Override
        public void destroy() {

        }
        
        
        private void validateParameter() {
        	originalConfig.getNecessaryValue(KeyConstant.TOPIC, MetaqWriterErrorCode.REQUIRED_VALUE);
        	originalConfig.getNecessaryValue(KeyConstant.NULL_FORMAT, MetaqWriterErrorCode.REQUIRED_VALUE);
        	originalConfig.getNecessaryValue(KeyConstant.PRODUCER_GROUP, MetaqWriterErrorCode.REQUIRED_VALUE);
        }

    }


    public static class Task extends Writer.Task {

        private Configuration writerSliceConfig;
        private String topic;
        private String tag;
        private String encoding = "utf-8";
        private char fieldDelimiter = '\t';
        private long failedLineCounter = 0;
        private boolean metaqNeedSendOk = false;
        private double errorLimit = 1000;
        private String nullFormat = null;
        private boolean exitWhenError = false;
        private String producerGroup = null;//默认无分组
        private int keyIndex = -1;//默认为-1,不需要key
        private static double EPSILON = 0.000001;

        private static int numOfPrintErrorLine = 10;
        private int printedErrorLineNum = 0;

        private  MetaProducer producer = null;
        
        private void processError(Record errorLine, Exception ex) {

            ++failedLineCounter;
            // 控制打印的错误日志数量
            if (printedErrorLineNum < numOfPrintErrorLine) {
                ++printedErrorLineNum;
                if (ex != null) {
                    logger.warn(String.format("error count: %d, line send failed: %s", failedLineCounter, errorLine.toString()), ex);
                } else {
                    logger.warn(String.format("error count: %d, line send failed: %s", failedLineCounter, errorLine.toString()));
                }
            }

            if (exitWhenError) {
            	throw  DataXException.asDataXException(MetaqWriterErrorCode.METAQWRITER_ERROR, MetaqWriterErrorCode.METAQWRITER_ERROR.getDescription());
            }
            if (errorLimit > =1 && failedLineCounter >= errorLimit) {
                logger.error(String.format("error count exceeded %d times!", failedLineCounter));
            	throw  DataXException.asDataXException(MetaqWriterErrorCode.METAQWRITER_ERROR, MetaqWriterErrorCode.METAQWRITER_ERROR.getDescription());
            }
        }

        @Override
		public void startWrite(RecordReceiver lineReceiver) {

			logger.info("start write to procuderGroup {} ,topic {}, with tag {}", new Object[] {
					producerGroup,topic, tag });

			Record line;
			while ((line = lineReceiver.getFromReader()) != null) {
				SendResult sendResult = null;
				try {
					String key = null;
					if(keyIndex>=0){
						key = String.valueOf(line.getColumn(keyIndex).getRawData());
					}
					Message msg = new Message(topic, tag, key,
							getMsgBytes(line));
					sendResult = producer.send(msg);
				} catch (Exception ex) {
					processError(line, ex);
					// 异常情况已处理
					continue;
				}
				// 正常，但不是SEND_OK
				if (metaqNeedSendOk && sendResult != null
						&& sendResult.getSendStatus() != SendStatus.SEND_OK) {
					processError(line, null);
				}
			}
		}

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            topic = writerSliceConfig.getString(KeyConstant.TOPIC);
            tag = writerSliceConfig.getString(KeyConstant.TAG, null);
            encoding = writerSliceConfig.getString(KeyConstant.ENCODING, encoding);
            fieldDelimiter = writerSliceConfig.getChar(KeyConstant.FIELD_DELIMITER, fieldDelimiter);
            metaqNeedSendOk = writerSliceConfig.getBool(KeyConstant.METAQ_NEED_SEND_OK, metaqNeedSendOk);
            nullFormat = writerSliceConfig.getString(KeyConstant.NULL_FORMAT, nullFormat);
            producerGroup = writerSliceConfig.getString(KeyConstant.PRODUCER_GROUP);
            keyIndex = writerSliceConfig.getInt(KeyConstant.KEY_INDEX, keyIndex);
            errorLimit = writerSliceConfig.getDouble(KeyConstant.ERROR_LIMIT, errorLimit);
            exitWhenError = Math.abs(errorLimit - 1L) < EPSILON;
            
			producer = new MetaProducer(producerGroup);
            try {
                // 如果需要send ok, 则当返回状态不是sendok时，尝试另外一个broker
                if (metaqNeedSendOk) {
                    producer.setRetryAnotherBrokerWhenNotStoreOK(true);
                }
                producer.start();
            } catch (MQClientException ex) {
                logger.error("metaq producer start failed!" + ex.getMessage(), ex);
                throw  DataXException.asDataXException(MetaqWriterErrorCode.MQClIENT_EXCEPTION,MetaqWriterErrorCode.MQClIENT_EXCEPTION.getDescription());
            }
            
        }

        @Override
        public void destroy() {
        	if(producer!=null)
        		producer.shutdown();
        }

		private byte[] getMsgBytes(Record line)
				throws UnsupportedEncodingException {

			int filedNum = line.getColumnNumber();
			StringBuffer sb = new StringBuffer();
			
			for (int i = 0; i < filedNum; i++) {
				Object obj = line.getColumn(i).getRawData();
				if(obj==null)
					obj = nullFormat;
				if(i!=0){
					sb.append(fieldDelimiter);
				}
				sb.append(obj);
			}
			return sb.toString().getBytes(encoding);
		}
    }

}
