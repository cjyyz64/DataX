package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.TableMeta;

public class ParamSplitPointParseFunctiontest {
    private static String tableName = "ots_reader_proxy_split_parse_functiontest";
    private static Configuration p = Utils.loadConf();
    private static final Logger LOG = LoggerFactory.getLogger(ParamSplitPointParseFunctiontest.class);

    /**
     * PartitionKey为 String时
     * @throws Exception 
     */
    @Test
    public void testCheckParam_split_for_string() throws Exception {
        {
            OTSClient ots = new OTSClient(p.getString("endpoint"), p.getString("accessid"), p.getString("accesskey"), p.getString("instance-name"));
            
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("Gid", PrimaryKeyType.STRING);
            Utils.createTable(ots, tableName, tableMeta);
            ots.shutdown();
        }
        
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        
        {
            String [][] input = {
                    // split的边界超过了begin和end的范围
                    {
                        "[{\"type\":\"string\", \"value\":\"0\"},{\"type\":\"string\", \"value\":\"9\"}]",
                        "The item of 'range-split' is not within scope of 'range-begin' and 'range-end'."
                    },
                    // split 的所有点和begin，end没有交集
                    {
                        "[{\"type\":\"string\", \"value\":\"10\"}, {\"type\":\"string\", \"value\":\"20\"},{\"type\":\"string\", \"value\":\"中国\"}]",
                        "The item of 'range-split' is not within scope of 'range-begin' and 'range-end'."
                    },
                    // split 的部分点和begin，end有交集，左边交集
                    //     [55~88)
                    // [00~69]
                    {
                        "[{\"type\":\"string\", \"value\":\"00\"}, {\"type\":\"string\", \"value\":\"66\"},{\"type\":\"string\", \"value\":\"69\"}]",
                        "The item of 'range-split' is not within scope of 'range-begin' and 'range-end'."
                    },
                    // split 的部分点和begin，end有交集，右边交集
                    // [55~88)
                    //    [69~99]
                    {
                        "[{\"type\":\"string\", \"value\":\"69\"},{\"type\":\"string\", \"value\":\"99\"}]",
                        "The item of 'range-split' is not within scope of 'range-begin' and 'range-end'."
                    },
                    // split 中的点类型不一致,如：{\"type\":\"string\", \"value\":\"56\"},{\"type\":\"int\", \"value\":\"66\"}
                    {
                        "[{\"type\":\"string\", \"value\":\"56\"},{\"type\":\"int\", \"value\":\"66\"}]",
                        "Not same column type, column1:STRING, column2:INTEGER"
                    },
                    // split 中的点有重复点, 如：{\"type\":\"string\", \"value\":\"66\"},{\"type\":\"string\", \"value\":\"66\"}
                    {
                        "[{\"type\":\"string\", \"value\":\"66\"},{\"type\":\"String\", \"value\":\"66\"}]",
                        "Multi same column in 'range-split'."
                    },
                    // split 中的点格式不对, {\"name\":\"string\", \"value\":\"66\"}
                    {
                        "[{\"name\":\"string\", \"value\":\"66\"}]",
                        "The map must consist of 'type' and 'value'."
                    },
                    // split 中的点格式不对, {\"type\":\"int\", \"value\":\"\"}
                    {
                        "[{\"type\":\"int\", \"value\":\"\"}]",
                        "Can not parse the value '' to Int."
                    },
                    // split 中的点格式不对, {\"type\":\"int\", \"value\":\"hello\"}
                    {
                        "[{\"type\":\"int\", \"value\":\"hello\"}]",
                        "Can not parse the value 'hello' to Int."
                    }
            };
            
            for (int i = 0; i < input.length; i++) {
                LOG.info("Split:{}, Message:{}", input[i][0], input[i][1]);
                String json = 
                        "{\"accessId\":\""+ p.getString("accessid") +"\","
                                + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                                + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                                + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                                + "\"column\":[{\"name\":\"xxxx\"}],"
                                + "\"range\":{"
                                +    "\"begin\":[{\"type\":\"string\", \"value\":\"55\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"}],"
                                +    "\"end\":  [{\"type\":\"string\", \"value\":\"88\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"}],"
                                +    "\"split\":" + input[i][0]
                                        + "},"
                                        + "\"table\":\""+ tableName +"\"}";

                Configuration p = Configuration.from(json);
                try {
                    proxy.init(p);
                    assertTrue(false);
                } catch (IllegalArgumentException e) {
                    assertEquals(input[i][1], e.getMessage());
                }
            }
        }
    }
    
    /**
     * PartitionKey为 Integer时
     * @throws Exception 
     */
    @Test
    public void testCheckParam_split_for_integer() throws Exception {
        {
            OTSClient ots = new OTSClient(p.getString("endpoint"), p.getString("accessid"), p.getString("accesskey"), p.getString("instance-name"));
            
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("Gid", PrimaryKeyType.STRING);
            Utils.createTable(ots, tableName, tableMeta);
            ots.shutdown();
        }
        
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        
        {
            String [][] input = {
                    // split的边界超过了begin和end的范围
                    {
                        "[{\"type\":\"int\", \"value\":\"-100\"},{\"type\":\"int\", \"value\":\"1000\"}]",
                        "The item of 'range-split' is not within scope of 'range-begin' and 'range-end'."
                    },
                    // split 的所有点和begin，end没有交集
                    {
                        "[{\"type\":\"int\", \"value\":\"10\"}, {\"type\":\"int\", \"value\":\"20\"},{\"type\":\"int\", \"value\":\"30\"}]",
                        "The item of 'range-split' is not within scope of 'range-begin' and 'range-end'."
                    },
                    // split 的部分点和begin，end有交集，左边交集
                    //     [55~88)
                    // [0~69]
                    {
                        "[{\"type\":\"int\", \"value\":\"0\"}, {\"type\":\"int\", \"value\":\"66\"},{\"type\":\"int\", \"value\":\"69\"}]",
                        "The item of 'range-split' is not within scope of 'range-begin' and 'range-end'."
                    },
                    // split 的部分点和begin，end有交集，右边交集
                    // [55~88)
                    //    [69~99]
                    {
                        "[{\"type\":\"int\", \"value\":\"69\"},{\"type\":\"int\", \"value\":\"99\"}]",
                        "The item of 'range-split' is not within scope of 'range-begin' and 'range-end'."
                    },
                    // split 中的点类型不一致
                    {
                        "[{\"type\":\"int\", \"value\":\"56\"},{\"type\":\"String\", \"value\":\"66\"}]",
                        "Not same column type, column1:INTEGER, column2:STRING"
                    },
                    // split 中的点有重复点
                    {
                        "[{\"type\":\"int\", \"value\":\"66\"},{\"type\":\"int\", \"value\":\"66\"}]",
                        "Multi same column in 'range-split'."
                    },
                    // split 中的点格式不对
                    {
                        "[{\"name\":\"int\", \"value\":\"66\"}]",
                        "The map must consist of 'type' and 'value'."
                    },
                    // split 中的点格式不对
                    {
                        "[{\"type\":\"int\", \"value\":\"\"}]",
                        "Can not parse the value '' to Int."
                    },
                    // split 中的点格式不对
                    {
                        "[{\"type\":\"int\", \"value\":\"hello\"}]",
                        "Can not parse the value 'hello' to Int."
                    }
            };
            
            for (int i = 0; i < input.length; i++) {
                LOG.info("Split:{}, Message:{}", input[i][0], input[i][1]);
                String json = 
                        "{\"accessId\":\""+ p.getString("accessid") +"\","
                                + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                                + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                                + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                                + "\"column\":[{\"name\":\"xxxx\"}],"
                                + "\"range\":{"
                                +    "\"begin\":[{\"type\":\"int\", \"value\":\"55\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"}],"
                                +    "\"end\":  [{\"type\":\"int\", \"value\":\"88\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"}],"
                                +    "\"split\":" + input[i][0]
                                        + "},"
                                        + "\"table\":\""+ tableName +"\"}";

                Configuration p = Configuration.from(json);
                try {
                    proxy.init(p);
                    assertTrue(false);
                } catch (IllegalArgumentException e) {
                    assertEquals(input[i][1], e.getMessage());
                }
            }
        }
    }
    
}
