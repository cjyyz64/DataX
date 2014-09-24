package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderSlaveProxy;
import com.aliyun.openservices.ClientException;
import com.aliyun.openservices.ots.OTSException;

public class OtsReader extends Reader {

    public static class Master extends Reader.Master {
        private static final Logger LOG = LoggerFactory.getLogger(OtsReader.Master.class);
        private OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        @Override
        public void init() {
            LOG.info("init() begin ...");
            try {
                this.proxy.init(getPluginJobConf());
            } catch (OTSException e) {
                LOG.error(e.getMessage());
                throw new DataXException(new OtsReaderError(e.getErrorCode()), e.getMessage(), e);
            } catch (ClientException e) {
                LOG.error(e.getMessage());
                throw new DataXException(new OtsReaderError(e.getErrorCode()), e.getMessage(), e);
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage());
                throw new DataXException(OtsReaderError.INVALID_PARAM, e.getMessage(), e);
            } catch (Exception e) {
                LOG.error(e.getMessage());
                throw new DataXException(OtsReaderError.ERROR, e.getMessage(), e);
            }
            LOG.info("init() end ...");
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("split() begin ...");

            if (adviceNumber <= 0) {
                throw new DataXException(OtsReaderError.ERROR, "Datax input adviceNumber <= 0.");
            }

            List<Configuration> confs = null;

            try {
                confs = this.proxy.split(adviceNumber);
            } catch (OTSException e) {
                LOG.error(e.getMessage());
                throw new DataXException(new OtsReaderError(e.getErrorCode()), e.getMessage(), e);
            } catch (ClientException e) {
                LOG.error(e.getMessage());
                throw new DataXException(new OtsReaderError(e.getErrorCode()), e.getMessage(), e);
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage());
                throw new DataXException(OtsReaderError.INVALID_PARAM, e.getMessage(), e);
            } catch (Exception e) {
                LOG.error(e.getMessage());
                throw new DataXException(OtsReaderError.ERROR, e.getMessage(), e);
            }

            LOG.info("split() end ...");
            return confs;
        }
    }

    public static class Slave extends Reader.Slave {
        private static final Logger LOG = LoggerFactory.getLogger(OtsReader.Slave.class);
        private OtsReaderSlaveProxy proxy = new OtsReaderSlaveProxy();

        @Override
        public void init() {
        }

        @Override
        public void destroy() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("startRead() begin ...");
            try {
                this.proxy.read(recordSender,getPluginJobConf());
            } catch (OTSException e) {
                LOG.error(e.getMessage());
                throw new DataXException(new OtsReaderError(e.getErrorCode()), e.getMessage(), e);
            } catch (ClientException e) {
                LOG.error(e.getMessage());
                throw new DataXException(new OtsReaderError(e.getErrorCode()), e.getMessage(), e);
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage());
                throw new DataXException(OtsReaderError.INVALID_PARAM, e.getMessage(), e);
            } catch (Exception e) {
                LOG.error(e.getMessage());
                throw new DataXException(OtsReaderError.ERROR, e.getMessage(), e);
            }
            LOG.info("startRead() end ...");
        }

    }
}
