package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;

/**
 * 请求满足特定条件就发送数据
 */
public class OTSSendBuffer {
    private int totalSize = 0;
    
    private OTSConf conf = null;
    private OTSBatchWriteRowTaskManager manager = null;

    private Map<PrimaryKey, OTSLine> linesForNormal = new HashMap<PrimaryKey, OTSLine>();
    private List<OTSLine> linesForMutli = new ArrayList<OTSLine>();
    

    public OTSSendBuffer(
            OTS ots,
            TaskPluginCollector collector,
            OTSConf conf) {
        this.conf = conf;
        this.manager = new OTSBatchWriteRowTaskManager(ots, collector, conf);
    }
    
    private void writeForNormal(OTSLine line) throws Exception {
        // 检查是否满足发送条件
        if (linesForNormal.size() >= conf.getBatchWriteCount() || 
           ((totalSize + line.getDataSize()) > conf.getRestrictConf().getRequestTotalSizeLimitation() && totalSize > 0)
           ) {
            manager.execute(new ArrayList<OTSLine>(linesForNormal.values()));
            linesForNormal.clear();
            totalSize = 0;
        }
        
        PrimaryKey oPk = line.getPk();
        OTSLine old = linesForNormal.get(oPk);
        if (old != null) {
            totalSize -= old.getDataSize(); // 移除相同PK的行的数据大小
        }
        linesForNormal.put(oPk, line);
        totalSize += line.getDataSize();
    }
    
    private void writeForMulti(OTSLine line) throws Exception {
        // 检查是否满足发送条件
        if (linesForNormal.size() >= conf.getBatchWriteCount() || 
           ((totalSize + line.getDataSize()) > conf.getRestrictConf().getRequestTotalSizeLimitation() && totalSize > 0)
           ) {
            manager.execute(new ArrayList<OTSLine>(linesForMutli));
            linesForMutli.clear();
            totalSize = 0;
        }
        linesForMutli.add(line);
        totalSize += line.getDataSize();
    }

    public void write(OTSLine line) throws Exception {
        if (conf.getMode() == OTSMode.NORMAL) {
            writeForNormal(line);
        } else {
            writeForMulti(line);
        }
    }

    public void flush() throws Exception {
        // 发送最后剩余的数据
        if (!linesForNormal.isEmpty()) {
            manager.execute(new ArrayList<OTSLine>(linesForNormal.values()));
        }
        if (!linesForMutli.isEmpty()) {
            manager.execute(new ArrayList<OTSLine>(linesForMutli));
        }
    }

    public void close() throws Exception {
        try {
            flush();
        } finally {
            manager.close();
        }
    }
}
