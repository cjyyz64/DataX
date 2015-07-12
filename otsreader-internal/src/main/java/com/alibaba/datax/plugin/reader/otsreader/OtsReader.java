package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSMode;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.aliyun.openservices.ots.internal.ClientException;
import com.aliyun.openservices.ots.internal.OTSException;

public class OtsReader {
    public static class Job extends Reader.Job  {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        
        @Override
        public void init() {
            LOG.info("init() begin ...");
            try {
                this.proxy.init(getPluginJobConf());
            } catch (OTSException e) {
                LOG.error("OTSException: {}",  e.toString(), e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}",  e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }
            LOG.info("init() end ...");
        }

        @Override
        public void destroy() {
            this.proxy.close();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            try {
                return this.proxy.split(mandatoryNumber);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.getMessage(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }
        }
    }
    
    public static class Task extends Reader.Task  {
        
        private OtsReaderSlaveProxy proxy = null;
        
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        
        private void initProxy() {
            OTSConf conf = GsonParser.jsonToConf((String) this.getPluginJobConf().get(Constant.KEY.CONF));
            OTSRange range = GsonParser.jsonToRange((String) this.getPluginJobConf().get(Constant.KEY.RANGE));
            
            if (conf.getMode() == OTSMode.MULTI_VERSION) {
                LOG.debug("Instance OtsReaderMultiVersionSlaveProxy");
                proxy = new OtsReaderMultiVersionSlaveProxy();
            } else {
                LOG.debug("Instance OtsReaderNormalSlaveProxy");
                proxy = new OtsReaderNormalSlaveProxy();
            }
            proxy.init(conf, range);
        }

        @Override
        public void init() {
            try {
                this.initProxy();
            } catch (OTSException e) {
                LOG.error("OTSException: {}",  e.toString(), e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}",  e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }
        }

        @Override
        public void destroy() {
            try {
                this.proxy.close();
            } catch (OTSException e) {
                LOG.error("OTSException: {}",  e.toString(), e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}",  e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                this.proxy.startRead(recordSender);
            } catch (OTSException e) {
                LOG.error("OTSException: {}",  e.toString(), e);
                throw DataXException.asDataXException(new OtsReaderError(e.getErrorCode(), "OTS端的错误"), e.toString(), e);
            } catch (ClientException e) {
                LOG.error("ClientException: {}",  e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            } catch (Exception e) {
                LOG.error("Exception. ErrorMsg:{}", e.toString(), e);
                throw DataXException.asDataXException(OtsReaderError.ERROR, e.toString(), e);
            }
        }
    }
}
