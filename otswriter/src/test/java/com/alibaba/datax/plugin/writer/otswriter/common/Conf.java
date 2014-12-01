package com.alibaba.datax.plugin.writer.otswriter.common;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSAttrColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSOpType;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSConf.RestrictConf;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;

public class Conf {

    public static OTSConf getConf(String tableName, List<PrimaryKeyType> pk, List<ColumnType> attr, OTSOpType op) {

        BaseTest base = new BaseTest(tableName);
        Configuration p = base.getP();

        OTSConf conf = new OTSConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccessKey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);

        List<OTSPKColumn> primaryKeyColumn = new ArrayList<OTSPKColumn>();
        for (int i = 0; i < pk.size(); i++) {
            primaryKeyColumn.add(new OTSPKColumn("pk_" + i, pk.get(i)));
        }
        conf.setPrimaryKeyColumn(primaryKeyColumn);

        List<OTSAttrColumn> attributeColumn = new ArrayList<OTSAttrColumn>();
        for (int i = 0; i < attr.size(); i++) {
            attributeColumn.add(new OTSAttrColumn("attr_" + i, attr.get(i)));
        }
        conf.setAttributeColumn(attributeColumn);

        conf.setOperation(op);

        conf.setRetry(18);
        conf.setSleepInMilliSecond(100);
        conf.setBatchWriteCount(100);
        conf.setConcurrencyWrite(5);
        conf.setIoThreadCount(1);
        conf.setSocketTimeout(60000);
        conf.setConnectTimeout(60000);
        
        RestrictConf restrictConf = conf.new RestrictConf();
        restrictConf.setRequestTotalSizeLimition(1024*1024);
        conf.setRestrictConf(restrictConf);

        base.close();
        return conf;
    }
}
