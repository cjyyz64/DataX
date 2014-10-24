package com.alibaba.datax.plugin.writer.otswriter;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.utils.GsonParser;
import com.alibaba.datax.plugin.writer.otswriter.utils.ParamChecker;
import com.alibaba.datax.plugin.writer.otswriter.utils.RetryHelper;
import com.alibaba.datax.plugin.writer.otswriter.utils.WriterModelParser;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.TableMeta;

public class OtsWriterMasterProxy {
    
    private OTSConf conf = new OTSConf();
    
    private OTSClient ots = null;
    
    private TableMeta meta = null;
    
    private static final Logger LOG = LoggerFactory.getLogger(OtsWriterMasterProxy.class);
    
    /**
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {
        LOG.info("OTSWriter master parameter : {}", param.toJSON());
        
        // 默认参数
        conf.setRetry(18);
        conf.setSleepInMilliSecond(100);
        conf.setBatchWriteCount(10);

        // 必选参数
        conf.setEndpoint(ParamChecker.checkStringAndGet(param, Key.OTS_ENDPOINT)); 
        conf.setAccessId(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSID)); 
        conf.setAccessKey(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSKEY)); 
        conf.setInstanceName(ParamChecker.checkStringAndGet(param, Key.OTS_INSTANCE_NAME)); 
        conf.setTableName(ParamChecker.checkStringAndGet(param, Key.TABLE_NAME)); 
        
        ots = new OTSClient(
                this.conf.getEndpoint(),
                this.conf.getAccessId(),
                this.conf.getAccessKey(),
                this.conf.getInstanceName());
        
        meta = getTableMeta(ots, conf.getTableName());
        LOG.info("Table Meta : {}", GsonParser.metaToJson(meta));
        
        conf.setPrimaryKeyColumn(WriterModelParser.parseOTSPKColumnList(ParamChecker.checkListAndGet(param, Key.PRIMARY_KEY, true)));
        ParamChecker.checkPrimaryKey(meta, conf.getPrimaryKeyColumn());
        
        conf.setAttributeColumn(WriterModelParser.parseOTSAttrColumnList(ParamChecker.checkListAndGet(param, Key.COLUMN, true)));
        ParamChecker.checkAttribute(conf.getAttributeColumn());
        
        conf.setOperation(WriterModelParser.parseOTSOpType(ParamChecker.checkStringAndGet(param, Key.WRITE_MODE)));
        LOG.info("User input conf : {}", GsonParser.confToJson(this.conf));
    }
    
    public List<Configuration> split(int mandatoryNumber){
        LOG.info("Begin split.");
        LOG.info("MandatoryNumber : {}", mandatoryNumber);
        List<Configuration> configurations = new ArrayList<Configuration>();
        for (int i = 0; i < mandatoryNumber; i++) {
            Configuration configuration = Configuration.newDefault();
            configuration.set(OTSConst.OTS_CONF, GsonParser.confToJson(this.conf));
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

    private TableMeta getTableMeta(OTSClient ots, String tableName) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                conf.getRetry(),
                conf.getSleepInMilliSecond()
                );
    }
}
