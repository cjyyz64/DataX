package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.utils.OtsHelper;
import com.alibaba.datax.plugin.reader.otsreader.utils.ParamChecker;
import com.aliyun.openservices.ots.internal.OTSClient;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class OtsReaderMasterProxy {
    
    private OTSConf conf = null;
    private TableMeta meta = null;
    private OTSClient ots = null;
    
    /**
     * 基于配置传入的配置文件，解析为对应的参数
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {
        // 基于预定义的Json格式,检查传入参数是否符合Conf定义规范
        conf = OTSConf.load(param);
        
        // Init ots
        ots = new OTSClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstanceName()
                );
        
        // 获取TableMeta
        meta = OtsHelper.getTableMeta(
                ots, 
                conf.getTableName(), 
                conf.getRetry(), 
                conf.getSleepInMilliSecond());
        
        // 基于Meta检查Conf是否正确
        ParamChecker.checkAndSetOTSConf(conf, meta);
    }
    
    public List<Configuration> split(int mandatoryNumber) {
        return null;
    }
    
    public void close(){
        ots.shutdown();
    }
}
