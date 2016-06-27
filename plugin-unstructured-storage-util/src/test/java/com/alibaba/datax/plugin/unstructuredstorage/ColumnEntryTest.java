package com.alibaba.datax.plugin.unstructuredstorage;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;

public class ColumnEntryTest {
    @Test
    public void testColumnEntry() {
        String config = "{'path':'/user/hive/warehouse/test3.db/odps/xinlang*??.txt','defaultFS':'hdfs://10.101.204.12:9000','column':[{'index':0,'type':'string','format':'yyyyMMdd'},{'value':'yixiao','type':'string'},{'index':2,'type':'string'},{'index':3,'type':'string'},{'index':4,'type':'string'},{'index':5,'type':'string'},{'index':6,'type':'string'},{'index':7,'type':'string'},{'index':8,'type':'string'},{'index':9,'type':'string'},{'index':10,'type':'string'},{'index':11,'type':'string'}],'fileType':'TEXT','encoding':'UTF-8','fieldDelimiter':','}";
        Configuration readerSliceConfig = Configuration.from(config);

        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, Key.COLUMN);
        List<String> result = new ArrayList<String>();
        for (ColumnEntry each : column) {
            String message = String.format(
                    "format:%s type:%s value:%s index:%s", each.getFormat(),
                    each.getType(), each.getValue(), each.getIndex());
            System.out.println(message);
            result.add(message);
        }

        String[] forChecks = new String[] {
                "format:yyyyMMdd type:string value:null index:0",
                "format:null type:string value:yixiao index:null",
                "format:null type:string value:null index:2",
                "format:null type:string value:null index:3",
                "format:null type:string value:null index:4",
                "format:null type:string value:null index:5",
                "format:null type:string value:null index:6",
                "format:null type:string value:null index:7",
                "format:null type:string value:null index:8",
                "format:null type:string value:null index:9",
                "format:null type:string value:null index:10",
                "format:null type:string value:null index:11" };
        String forChecksStr = StringUtils.join(forChecks, ", ");
        String resultStr = StringUtils.join(result, ", ");
        Assert.assertTrue(forChecksStr.equals(resultStr));
    }
}
