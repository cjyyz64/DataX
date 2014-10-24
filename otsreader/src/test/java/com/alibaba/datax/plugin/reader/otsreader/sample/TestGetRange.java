package com.alibaba.datax.plugin.reader.otsreader.sample;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

public class TestGetRange {

    public static void main(String[] args) {
        OTSClient ots = new OTSClient(
                "http://10.101.168.117", 
                "dataxaccessid", 
                "dataxaccesskey", 
                "dataxtest");
        
        RowPrimaryKey inclusiveStartPrimaryKey = new RowPrimaryKey();
        inclusiveStartPrimaryKey.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MIN);
        inclusiveStartPrimaryKey.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MIN);
        inclusiveStartPrimaryKey.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MIN);
        inclusiveStartPrimaryKey.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MIN);
        //inclusiveStartPrimaryKey.addPrimaryKeyColumn("userid", PrimaryKeyValue.fromString("0"));
        //inclusiveStartPrimaryKey.addPrimaryKeyColumn("groupid", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey exclusiveEndPrimaryKey = new RowPrimaryKey();
        exclusiveEndPrimaryKey.addPrimaryKeyColumn("pk_0", PrimaryKeyValue.INF_MAX);
        exclusiveEndPrimaryKey.addPrimaryKeyColumn("pk_1", PrimaryKeyValue.INF_MAX);
        exclusiveEndPrimaryKey.addPrimaryKeyColumn("pk_2", PrimaryKeyValue.INF_MAX);
        exclusiveEndPrimaryKey.addPrimaryKeyColumn("pk_3", PrimaryKeyValue.INF_MAX);
        //exclusiveEndPrimaryKey.addPrimaryKeyColumn("userid", PrimaryKeyValue.fromString("\\\\:"));
        //exclusiveEndPrimaryKey.addPrimaryKeyColumn("groupid", PrimaryKeyValue.INF_MIN);
        
        List<String> columnsToGet = new ArrayList<String>();
        //columnsToGet.add("pk_41");
        columnsToGet.add("attr_12");
        
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria("ots_reader_column_mix");
        criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        criteria.setColumnsToGet(columnsToGet);
        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        
        System.out.println("Begin");
        GetRangeResult r = ots.getRange(request);
        System.out.println(r.getRows().size());
        System.out.println("End");
        ots.shutdown();
    }

}
