package com.alibaba.datax.plugin.writer.otswriter;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf.RestrictConf;
import com.alibaba.datax.plugin.writer.otswriter.utils.Common;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParamChecker;
import com.alibaba.datax.plugin.writer.otswriter.utils.RetryHelper;
import com.alibaba.datax.plugin.writer.otswriter.utils.WriterModelParser;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class OtsWriterMasterProxy {
    
    private OTSConf conf = new OTSConf();
    
    private OTS ots = null;
    
    private TableMeta meta = null;
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterMasterProxy.class);
    
    /**
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {
        
        // 默认参数
        conf.setRetry(param.getInt(OTSConst.RETRY, 18));
        conf.setSleepInMillisecond(param.getInt(OTSConst.SLEEP_IN_MILLISECOND, 100));
        conf.setBatchWriteCount(param.getInt(OTSConst.BATCH_WRITE_COUNT, 100));
        conf.setConcurrencyWrite(param.getInt(OTSConst.CONCURRENCY_WRITE, 5));
        conf.setIoThreadCount(param.getInt(OTSConst.IO_THREAD_COUNT, 1));
        conf.setSocketTimeoutInMillisecond(param.getInt(OTSConst.SOCKET_TIMEOUTIN_MILLISECOND, 10000));
        conf.setConnectTimeoutInMillisecond(param.getInt(OTSConst.CONNECT_TIMEOUT_IN_MILLISECOND, 10000));
        
        RestrictConf restrictConf = conf.new RestrictConf();
        restrictConf.setRequestTotalSizeLimitation(param.getInt(OTSConst.REQUEST_TOTAL_SIZE_LIMITATION, 1024*1024));
        restrictConf.setRowCellCountLimitation(param.getInt(OTSConst.ROW_CELL_COUNT_LIMITATION, 128));
        conf.setRestrictConf(restrictConf);

        conf.setTimestamp(param.getInt(Key.DEFAULT_TIMESTAMP, -1));

        // 必选参数
        conf.setEndpoint(ParamChecker.checkStringAndGet(param, Key.OTS_ENDPOINT)); 
        conf.setAccessId(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSID)); 
        conf.setAccessKey(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSKEY)); 
        conf.setInstanceName(ParamChecker.checkStringAndGet(param, Key.OTS_INSTANCE_NAME)); 
        conf.setTableName(ParamChecker.checkStringAndGet(param, Key.TABLE_NAME));
        
        conf.setMode(WriterModelParser.parseOTSMode(ParamChecker.checkStringAndGet(param, Key.MODE)));
        conf.setOperation(WriterModelParser.parseOTSOpType(ParamChecker.checkStringAndGet(param, Key.WRITE_MODE), conf.getMode()));
        
        ots = Common.getOTSInstance(conf);
        
        meta = getTableMeta(ots, conf.getTableName());
        LOG.debug("Table Meta : {}", GsonParser.metaToJson(meta));
        
        conf.setPrimaryKeyColumn(WriterModelParser.parseOTSPKColumnList(ParamChecker.checkListAndGet(param, Key.PRIMARY_KEY, true)));
        ParamChecker.checkPrimaryKey(meta, conf.getPrimaryKeyColumn());
        
        conf.setAttributeColumn(
                WriterModelParser.parseOTSAttrColumnList(
                        conf.getPrimaryKeyColumn(),
                        ParamChecker.checkListAndGet(param, Key.COLUMN, true),
                        conf.getMode(),
                        restrictConf.getRowCellCountLimitation()
                )
        );
        ParamChecker.checkAttribute(conf.getAttributeColumn());
        
        conf.setEncodePkColumnMapping(Common.getEncodePkColumnMapping(meta, conf.getPrimaryKeyColumn()));
    }
    
    public List<Configuration> split(int mandatoryNumber){
        LOG.info("Begin split and MandatoryNumber : {}", mandatoryNumber);
        List<Configuration> configurations = new ArrayList<Configuration>();
        String json = GsonParser.confToJson(this.conf);
        for (int i = 0; i < mandatoryNumber; i++) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(OTSConst.OTS_CONF, json);
            configurations.add(configuration);
        }
        LOG.info("End split.");
        return configurations;
    }
    
    public void close() {
        ots.shutdown();
    }
    
    public OTSConf getOTSConf() {
        return conf;
    }

    // private function

    private TableMeta getTableMeta(OTS ots, String tableName) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                conf.getRetry(),
                conf.getSleepInMillisecond()
                );
    }
}
