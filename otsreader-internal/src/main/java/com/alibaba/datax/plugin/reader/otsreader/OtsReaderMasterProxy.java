package com.alibaba.datax.plugin.reader.otsreader;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsreader.utils.OtsHelper;
import com.alibaba.datax.plugin.reader.otsreader.utils.ParamChecker;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class OtsReaderMasterProxy {
    
    private OTSConf conf = null;
    private TableMeta meta = null;
    private OTS ots = null;

    public OTSConf getConf() {
        return conf;
    }

    public TableMeta getMeta() {
        return meta;
    }

    public OTS getOts() {
        return ots;
    }

    private static final Logger LOG = LoggerFactory.getLogger(OtsReaderMasterProxy.class);
    
    /**
     * 基于配置传入的配置文件，解析为对应的参数
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {
        // 基于预定义的Json格式,检查传入参数是否符合Conf定义规范
        conf = OTSConf.load(param);
        
        // Init ots
        ots = OtsHelper.getOTSInstance(conf);
        
        LOG.info(GsonParser.confToJson(conf));
        
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
        List<Configuration> configurations = getConfigurationBySplit();
        LOG.info("Expect split num: "+ mandatoryNumber +", and final configuration list count : " + configurations.size());
        return configurations;
    }
    
    public void close(){
        ots.shutdown();
    }
    
    /**
     * 根据用户配置的split信息，将配置文件基于Range范围转换为多个Task的配置
     */
    private List<Configuration> getConfigurationBySplit() {
        List<List<PrimaryKeyColumn>> primaryKeys = new ArrayList<List<PrimaryKeyColumn>>();
        primaryKeys.add(conf.getRange().getBegin());
        primaryKeys.addAll(conf.getRange().getSplit());
        primaryKeys.add(conf.getRange().getEnd());
        
        List<Configuration> configurations = new ArrayList<Configuration>(primaryKeys.size() - 1);
        
        for (int i = 0; i < primaryKeys.size() - 1; i++) {
            OTSRange range = new OTSRange();
            range.setBegin(primaryKeys.get(i));
            range.setEnd(primaryKeys.get(i + 1));
            
            Configuration configuration = Configuration.newDefault();
            configuration.set(Constant.KEY.CONF, GsonParser.confToJson(conf));
            configuration.set(Constant.KEY.RANGE, GsonParser.rangeToJson(range));
            configurations.add(configuration);
        }
        return configurations;
    }
}
