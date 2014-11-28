package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.Key;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.Person;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.TableMeta;

/**
 * 主要是测试参数合法性的检查
 * @author wanhong.chenwh@alibaba-inc.com
 *
 */
public class ParamParseFunctiontest {
    private static String tableName = "ots_reader_proxy_param_parse_functiontest";
    private static Configuration p = Utils.loadConf();
    
    private ReaderConf readerConf = null;
    
    private static final Logger LOG = LoggerFactory.getLogger(ParamParseFunctiontest.class);
    
    @BeforeClass
    public static void setBeforeClass() {
        OTSClient ots = new OTSClient(p.getString("endpoint"), p.getString("accessid"), p.getString("accesskey"), p.getString("instance-name"));
        
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("Uid", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("Pid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("Mid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("Gid", PrimaryKeyType.STRING);
        try {
            Utils.createTable(ots, tableName, tableMeta);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        ots.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        Person person = new Person();
        person.setName("为硬音k，近似普通话轻声以外的g: cum,cīvis,facilis");
        person.setAge(Long.MAX_VALUE);
        person.setHeight(1111);
        person.setMale(false);

        readerConf = new ReaderConf();
        OTSConf conf = new OTSConf();
        Configuration p = Utils.loadConf();
        conf.setEndpoint(p.getString("endpoint"));
        conf.setAccessId(p.getString("accessid"));
        conf.setAccesskey(p.getString("accesskey"));
        conf.setInstanceName(p.getString("instance-name"));
        conf.setTableName(tableName);

        List<OTSColumn> columns = new ArrayList<OTSColumn>();
        columns.add(OTSColumn.fromNormalColumn("col0"));
        columns.add(OTSColumn.fromNormalColumn("col1"));
        columns.add(OTSColumn.fromNormalColumn("col2"));
        columns.add(OTSColumn.fromNormalColumn("col2"));
        
        columns.add(OTSColumn.fromConstStringColumn(""));
        columns.add(OTSColumn.fromConstStringColumn("测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^"));
        columns.add(OTSColumn.fromConstIntegerColumn(100L));
        columns.add(OTSColumn.fromConstDoubleColumn(112111111.013112));
        columns.add(OTSColumn.fromConstIntegerColumn(Long.MIN_VALUE));
        columns.add(OTSColumn.fromConstIntegerColumn(Long.MAX_VALUE));
        columns.add(OTSColumn.fromConstBoolColumn(true));
        columns.add(OTSColumn.fromConstBoolColumn(false));
        columns.add(OTSColumn.fromConstBytesColumn(Person.toByte(person)));
        
        conf.setColumns(columns);

        List<PrimaryKeyValue> begin = new ArrayList<PrimaryKeyValue>();
        begin.add(PrimaryKeyValue.INF_MAX);
        begin.add(PrimaryKeyValue.fromLong(2313443));
        begin.add(PrimaryKeyValue.INF_MIN);
        begin.add(PrimaryKeyValue.fromString("中国"));
        
        List<PrimaryKeyValue> end = new ArrayList<PrimaryKeyValue>();
        end.add(PrimaryKeyValue.INF_MIN);
        end.add(PrimaryKeyValue.fromLong(2313443));
        end.add(PrimaryKeyValue.INF_MIN);
        end.add(PrimaryKeyValue.fromString("中国"));
        
        List<PrimaryKeyValue> splits = new ArrayList<PrimaryKeyValue>();

        conf.setRangeBegin(begin);
        conf.setRangeEnd(end);
        conf.setRangeSplit(splits);

        readerConf.setConf(conf);
        
        Thread.sleep(1000);
    }

    /**
     * 基础功能测试，测试解析Json的正确性
     * 输入：构造满足要求的字段
     * 期望：系统能正常解析配置，且值符合预期
     * @throws Exception
     */
    @Test
    public void testBaseInit() throws Exception {

        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        Configuration p = Configuration.from(readerConf.toString());

        proxy.init(p);

        assertEquals(p.getString(Key.OTS_ENDPOINT), proxy.getConf().getEndpoint());
        assertEquals(p.getString(Key.OTS_ACCESSID), proxy.getConf().getAccessId());
        assertEquals(p.getString(Key.OTS_ACCESSKEY), proxy.getConf().getAccesskey());
        assertEquals(p.getString(Key.OTS_INSTANCE_NAME), proxy.getConf().getInstanceName());
        assertEquals(tableName, proxy.getConf().getTableName());
        assertEquals(18, proxy.getConf().getRetry());
        assertEquals(100, proxy.getConf().getSleepInMilliSecond());
        assertEquals(13, proxy.getConf().getColumns().size());
        assertEquals(4, proxy.getConf().getRangeBegin().size());
        assertEquals(4, proxy.getConf().getRangeEnd().size());
        assertEquals(0, proxy.getConf().getRangeSplit().size());

        List<OTSColumn>columns = proxy.getConf().getColumns();
        OTSColumn col0 = columns.get(0);
        assertEquals(OTSColumn.OTSColumnType.NORMAL, col0.getColumnType());
        assertEquals("col0", col0.getName());

        OTSColumn col1 = columns.get(1);
        assertEquals(OTSColumn.OTSColumnType.NORMAL, col1.getColumnType());
        assertEquals("col1", col1.getName());

        OTSColumn col2 = columns.get(2);
        assertEquals(OTSColumn.OTSColumnType.NORMAL, col2.getColumnType());
        assertEquals("col2", col2.getName());

        OTSColumn col3 = columns.get(3);
        assertEquals(OTSColumn.OTSColumnType.NORMAL, col3.getColumnType());
        assertEquals("col2", col3.getName());

        OTSColumn col4 = columns.get(4);
        assertEquals(OTSColumn.OTSColumnType.CONST, col4.getColumnType());
        assertEquals("", col4.getValue().asString());

        OTSColumn col5 = columns.get(5);
        assertEquals(OTSColumn.OTSColumnType.CONST, col5.getColumnType());
        assertEquals("测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^", col5.getValue().asString());

        OTSColumn col6 = columns.get(6);
        assertEquals(OTSColumn.OTSColumnType.CONST, col6.getColumnType());
        assertEquals(100, col6.getValue().asLong().longValue());

        OTSColumn col7 = columns.get(7);
        assertEquals(OTSColumn.OTSColumnType.CONST, col7.getColumnType());
        assertEquals(true, 112111111.013112 == col7.getValue().asDouble());
        
        OTSColumn col8 = columns.get(8);
        assertEquals(OTSColumn.OTSColumnType.CONST, col8.getColumnType());
        assertEquals(true, Long.MIN_VALUE == col8.getValue().asLong());
        
        OTSColumn col9 = columns.get(9);
        assertEquals(OTSColumn.OTSColumnType.CONST, col9.getColumnType());
        assertEquals(true, Long.MAX_VALUE == col9.getValue().asLong());

        OTSColumn col10 = columns.get(10);
        assertEquals(OTSColumn.OTSColumnType.CONST, col10.getColumnType());
        assertEquals(true, col10.getValue().asBoolean());

        OTSColumn col11 = columns.get(11);
        assertEquals(OTSColumn.OTSColumnType.CONST, col11.getColumnType());
        assertEquals(false, col11.getValue().asBoolean());

        OTSColumn col12 = columns.get(12);
        assertEquals(OTSColumn.OTSColumnType.CONST, col12.getColumnType());

        Person newP = Person.toPerson(col12.getValue().asBytes());
        assertEquals("为硬音k，近似普通话轻声以外的g: cum,cīvis,facilis", newP.getName());
        assertEquals(Long.MAX_VALUE, newP.getAge());
        assertEquals(true, newP.getHeight() == 1111.0);
        assertEquals(false, newP.isMale());
    }

    /**
     * 测试endpoint为空和值为空字符串的情况
     * @throws Exception
     */
    @Test
    public void testCheckParam_endpoint() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        // 输入：endpoint字段不存在
        // 期望：异常退出
        {
            readerConf.getConf().setEndpoint(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'endpoint' is not exist.", e.getMessage());
            }
        }
        // size == 0
        // 输入：endpoint字段的值为空字符串
        // 期望：异常退出
        {
            readerConf.getConf().setEndpoint("");
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param length of 'endpoint' is zero.", e.getMessage());
            }
        }
    }

    /**
     * 测试accessid为空和值为空字符串的情况
     * @throws Exception
     */
    @Test
    public void testCheckParam_accessid() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        // 输入：accessId字段不存在
        // 期望：异常退出
        {
            readerConf.getConf().setAccessId(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'accessId' is not exist.", e.getMessage());
            }
        }
        // size == 0
        // 输入：accessId字段的值为空字符串
        // 期望：异常退出
        {
            readerConf.getConf().setAccessId("");
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param length of 'accessId' is zero.", e.getMessage());
            }
        }
    }

    /**
     * 测试accesskey为空和值为空字符串的情况
     * 输入：
     * 期望：
     * @throws Exception
     */
    @Test
    public void testCheckParam_accesskey() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        // 输入：accessKey字段不存在
        // 期望：异常退出
        {
            readerConf.getConf().setAccesskey(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'accessKey' is not exist.", e.getMessage());
            }
        }
        // size == 0
        // 输入：accessKey字段的值为空字符串
        // 期望：异常退出
        {
            readerConf.getConf().setAccesskey("");
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param length of 'accessKey' is zero.", e.getMessage());
            }
        }
    }

    /**
     * 测试instancename为空和值为空字符串的情况
     * @throws Exception
     */
    @Test
    public void testCheckParam_instancename() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        // 输入：instanceName字段不存在
        // 期望：异常退出
        {
            readerConf.getConf().setInstanceName(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'instanceName' is not exist.", e.getMessage());
            }
        }
        // size == 0
        // 输入：instanceName字段的值为空字符串
        // 期望：异常退出
        {
            readerConf.getConf().setInstanceName("");
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param length of 'instanceName' is zero.", e.getMessage());
            }
        }
    }

    /**
     * 测试table为空和值为空字符串的情况
     * @throws Exception
     */
    @Test
    public void testCheckParam_table() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        // 输入：table字段不存在
        // 期望：异常退出
        {
            readerConf.getConf().setTableName(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'table' is not exist.", e.getMessage());
            }
        }
        // size == 0
        // 输入：table字段的值为空字符串
        // 期望：异常退出
        {
            readerConf.getConf().setTableName("");
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param length of 'table' is zero.", e.getMessage());
            }
        }
    }
    
    /**
     * column参数合法性的检查
     * @throws Exception
     */
    @Test
    public void testCheckParam_column() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();

        // column不存在
        // 输入：column字段不存在
        // 期望：异常退出
        {
            readerConf.getConf().setColumns(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'column' is not exist.", e.getMessage());
            }
        }
        
        // 空的column
        // 输入：column = []
        // 期望：异常退出
        {
            List<OTSColumn> columns = new ArrayList<OTSColumn>();
            readerConf.getConf().setColumns(columns);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param  'column' is empty.", e.getMessage());
            }
        }
        
        // 构造非法的的column
        // 备注：每个子数组的第一列表示Column的输入，第二列表示异常的错误信息
        {
            String [][] input = {
                    {"\"somevalue\"",                                             "The param  'column' is not a json array."},
                    {"[{\"name\":\"\"}]",                                         "The column name is empty."},
                    {"[{\"name\": \"col\"}, \"col1\"]",                           "Invalid 'column', Can not parse Object to 'OTSColumn', item of list is not a map."},
                    {"[{\"invalid\": \"col\"}, {\"name\": \"col1\"}]",            "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"\": \"col\"}, {\"name\": \"col1\"}]",                   "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"name\": \"\"}, {\"name\": \"col1\"}]",                  "The column name is empty."},
                    {"[{\"name\": \"xx\"}, {\"name\": 1000}]",                    "Invalid 'column', Can not parse map to 'OTSColumn', the value is not a string."},
                    {"[{\"name\": \"col\"},{\"type\": \"STRING\"}]",              "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"name\": \"col\"},{\"value\":\"111\"}]",                 "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"name\": \"col\"},{\"name\":\"col\",\"type\":\"STRING\",\"value\":\"abc\"}]","Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"name\": \"col\"},{\"name\":\"col\",\"type\":\"STRING\"}]","Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"name\": \"col\"},{\"name\":\"col\",\"value\":\"abc\"}]", "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"invalid\": \"something\",\"type\":\"INT\",\"value\":\"1000\"}]", "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"invalid\": \"something\",\"type\":\"INT\"}]",           "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"invalid\": \"something\",\"value\":\"INT\"}]",          "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"type\": \"INTXXX\", \"name\": \"111\"}]",               "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"type\": \"\", \"name\": \"111\"}]",                     "Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"name\": \"col\"},{\"type\":\"INT\", \"value\":111}]",   "Invalid 'column', Can not parse map to 'OTSColumn', the value is not a string."},
                    {"[{\"name\": \"col\"},{\"type\":\"INT\", \"value\":\"aa\"}]","Can not parse the value 'aa' to Int."},
                    {"[{\"name\": \"col\"},{\"type\":\"INT\", \"value\":\"\"}]",  "Can not parse the value '' to Int."},
                    {"[{\"name\": \"col\"},{\"tpye\":\"STRING\",\"value\":\"\"}]","Invalid 'column', Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'."},
                    {"[{\"name\": \"col\"},{\"type\":\"BOOL\",\"value\":\"100\"}]","Can not parse the value '100' to Bool."},
                    {"[{\"type\":\"BOOL\",\"value\":\"TRUE\"}]",                  "Invalid 'column', 'column' should include at least one or more Normal Column."},
                    {"[{\"name\": \"col\"},{\"type\":\"xx\",\"value\":\"TRUE\"}]","Invalid 'column', Can not parse map to 'OTSColumn', input type:xx, value:TRUE."},
                    {"[{\"name\": \"col\"},{\"type\":\"BOOL\",\"value\":\"1\"}]", "Can not parse the value '1' to Bool."},
                    {"[{\"name\": \"col\"},{\"type\":\"BOOL\",\"value\":\"0\"}]", "Can not parse the value '0' to Bool."},
                    {"[{\"name\": \"col\"},{\"type\":\"BOOL\",\"value\":\"abc\"}]","Can not parse the value 'abc' to Bool."}
            };
            
            for (int i = 0; i < input.length; i++) {
                String value = input[i][0];
                String message = input[i][1];
                LOG.info("Json:{}, Message:{}", value, message);
                String json = 
                        "{\"accessId\":\""+ p.getString("accessid") +"\","
                                + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                                + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                                + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                                + "\"column\":"+ value +"," //point
                                + "\"range\":{"
                                + "  \"begin\":[],"
                                + "  \"end\":[],"
                                + "  \"split\":[]"
                                + "},"
                                + "\"table\":\""+ tableName +"\"}";
                
                Configuration p = Configuration.from(json);
                try {
                    proxy.init(p);
                    assertTrue(false);
                } catch (IllegalArgumentException e) {
                    assertEquals(message, e.getMessage());
                }
            }
        }
        
        // 构造合法的的column
        {
            String [] input = {
                    "[{\"name\": \"col\"}, {\"type\":\"StrIng\",\"value\":\"\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"STRING\",\"value\":\"\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"string\",\"value\":\"\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"Bool\",\"value\":\"false\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"bool\",\"value\":\"FalSE\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"BOOL\",\"value\":\"False\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"BOOL\",\"value\":\"TRUE\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"BOOL\",\"value\":\"true\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"BOOL\",\"value\":\"truE\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"double\", \"value\": \"1.00009\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"double\", \"value\": \"112121\"}]",
                    "[{\"name\": \"col\"}, {\"type\":\"DOUBLE\", \"value\": \"-1.00009\"}]"
            };
            
            for (int i = 0; i < input.length; i++) {
                String value = input[i];
                LOG.info("Json:{}", value);
                String json = 
                        "{\"accessId\":\""+ p.getString("accessid") +"\","
                                + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                                + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                                + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                                + "\"column\":"+ value +"," //point
                                + "\"range\":{"
                                + "  \"begin\":[],"
                                + "  \"end\":[],"
                                + "  \"split\":[]"
                                + "},"
                                + "\"table\":\""+ tableName +"\"}";
                
                Configuration p = Configuration.from(json);
                try {
                    proxy.init(p);
                    assertTrue(true);
                } catch (Exception e) {
                    LOG.info("Error:{}", e);
                    assertTrue(false);
                }
            }
        }   
    }
    
    /**
     * 测试对Range的检查
     * @throws Exception 
     */
    @Test
    public void testCheckParam_range() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // 测试range不存在
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'range' is not exist.", e.getMessage());
            }
        }
        // 测试range的值为空
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param  'range' is empty.", e.getMessage());
            }
        }
        
        // 测试begin不存在
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{\"end\":[]},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'begin' is not exist.", e.getMessage());
            }
        }
        
        // 测试end不存在
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{\"begin\":[]},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'end' is not exist.", e.getMessage());
            }
        }
        
        // split点的类型和Meta中的PartitionKey不一致
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{\"begin\":[], \"end\":[], \"split\":[{\"type\":\"int\", \"value\":\"900\"}]},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Input type of 'range-split' not match partition key. Item of 'range-split' type:INTEGER, Partition type:STRING", e.getMessage());
            }
        }
       
        {
            // 备注：每个子数组的第一列表示begin的输入，第二列表示end的输入，第三列表示异常的错误消息
            String [][] input = {
                    // 测试begin和end类型不一致
                    {
                        "[{\"type\":\"int\", \"value\":\"900\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"}]",
                        "[{\"type\":\"string\", \"value\":\"hello\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"},{\"type\":\"inf_min\"}]",
                        "Input range type not match primary key. Input type:INTEGER, Primary Key Type:STRING, Index:0"
                    },
                    // 5个PK Column
                    {
                        "[{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "Input size of values not equal size of primary key. input size:5, primary key size:4 .",
                    },
                    // range有4个正常的PK{...},一个有问题的{}
                    {
                        "[{\"type\":\"INF_MIN\", \"value\":\"\", \"invalid\":\"bug\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "The map must consist of 'type' and 'value'.",
                    },
                    // range中有非法PK，invalid key name
                    {
                        "[{\"type\":\"STRING\", \"name\":\"hello\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "The map must consist of 'type' and 'value'.",
                    },
                    {
                        "[{\"value\":\"87\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "The map must consist of 'type' and 'value'.",
                    },
                    {
                        "[{\"invalidtype\": \"STRING\",\"value\":\"1\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "The map must consist of 'type' and 'value'.",
                    },
                    {
                        "[{\"type\": \"STRING\", \"invalidvalue\": \"1\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "The map must consist of 'type' and 'value'.",
                    },
                    {
                        "[{\"invalid\":\"bug\",\"type\":\"INT\",\"value\":\"87\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "The map must consist of 'type' and 'value'.",
                    },
                    // range中有非法PK，invalid value
                    {
                        "[{\"type\":\"INVALID\", \"value\":\"1\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "Not supprot parsing type: INVALID for PrimaryKeyValue.",
                    },
                    {
                        "[{\"type\":\"BOOL\",\"value\":\"false\"} ,{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "Not supprot parsing type: BOOL for PrimaryKeyValue.",
                    },
                    {
                        "[{\"type\":\"INT\",\"value\":\"yzk\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}]",
                        "Can not parse the value 'yzk' to Int.",
                    },
                    // begin为空，end有值
                    {
                        "[]",
                        "[{\"type\":\"INF_MAX\"}]",
                        "Input size of values not equal size of primary key. input size:0, primary key size:4 .",
                    },
                    // begin有值，end为空
                    {
                        "[{\"type\":\"INF_MAX\"}]",
                        "[]",
                        "Input size of values not equal size of primary key. input size:1, primary key size:4 .",
                    },
                    // begin中的个数和end中的个数不同
                    {
                        "[{\"type\":\"INF_MAX\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "[{\"type\":\"INF_MIN\"},{\"type\":\"INF_MIN\"}]",
                        "Input size of values not equal size of primary key. input size:2, primary key size:4 .",
                    },
                    // begin和end中对应的类型不同
                    {
                        "[{\"type\":\"STRING\", \"value\":\"hello\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "[{\"type\":\"INT\", \"value\":\"1000\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "Input range type not match primary key. Input type:INTEGER, Primary Key Type:STRING, Index:0",
                    },
                    // 测试INF_MIN错误输入,如：{\"type\":\"INF_MIN\", \"value\":\"hello\"}
                    {
                        "[{\"type\":\"STRING\", \"value\":\"hello\"},{\"type\":\"INF_MIN\", \"value\":\"hello\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "[{\"type\":\"INT\", \"value\":\"1000\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "Format error, the INF_MIN only support {\"type\":\"INF_MIN\"}."
                    },
                    // 测试INF_MIN错误输入,如：{\"type\":\"INF_MIN\", \"value\":\"\"}
                    {
                        "[{\"type\":\"STRING\", \"value\":\"hello\"},{\"type\":\"INF_MIN\", \"value\":\"\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "[{\"type\":\"INT\", \"value\":\"1000\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "Format error, the INF_MIN only support {\"type\":\"INF_MIN\"}."
                    },
                    // 测试INF_MIN错误输入,如：{\"\":\"INF_MIN\"}
                    {
                        "[{\"type\":\"STRING\", \"value\":\"hello\"},{\"\":\"INF_MIN\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "[{\"type\":\"INT\", \"value\":\"1000\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "The map must consist of 'type' and 'value'."
                    },
                    // 测试INF_MAX错误输入,如：{\"type\":\"INF_MAX\", \"value\":\"hello\"}
                    {
                        "[{\"type\":\"STRING\", \"value\":\"hello\"},{\"type\":\"INF_MAX\", \"value\":\"hello\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "[{\"type\":\"INT\", \"value\":\"1000\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "Format error, the INF_MAX only support {\"type\":\"INF_MAX\"}."
                    },
                    // 测试INF_MAX错误输入,如：{\"type\":\"INF_MAX\", \"value\":\"\"}
                    {
                        "[{\"type\":\"STRING\", \"value\":\"hello\"},{\"type\":\"INF_MAX\", \"value\":\"\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "[{\"type\":\"INT\", \"value\":\"1000\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "Format error, the INF_MAX only support {\"type\":\"INF_MAX\"}."
                    },
                    // 测试INF_MAX错误输入,如：{\"\":\"INF_MAX\"}
                    {
                        "[{\"type\":\"STRING\", \"value\":\"hello\"},{\"\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "[{\"type\":\"INT\", \"value\":\"1000\"},{\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}, {\"type\":\"INF_MAX\"}]",
                        "The map must consist of 'type' and 'value'."
                    },
            };

            for (int i = 0; i < input.length; i++) {
                LOG.info("Begin:{}, End:{}, Message:{}", new String []{input[i][0], input[i][1], input[i][2]});
                String json = 
                        "{\"accessId\":\""+ p.getString("accessid") +"\","
                                + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                                + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                                + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                                + "\"column\":[{\"name\":\"xxxx\"}],"
                                + "\"range\":{"
                                +    "\"begin\":"+ input[i][0] +","
                                +    "\"end\":" + input[i][1]
                                + "},"
                                + "\"table\":\""+ tableName +"\"}";
                Configuration p = Configuration.from(json);
                try {
                    proxy.init(p);
                    assertTrue(false);
                } catch (IllegalArgumentException e) {
                    assertEquals(input[i][2], e.getMessage());
                }
            }
        }
    } 
    
    /**
     * split的参数检查测试
     * @throws Exception 
     */
    @Test
    public void testCheckParam_split() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // 输入：没有split的情况
        // 期望：程序解析正常，且split为null
        {
            readerConf.getConf().setRangeSplit(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(true);
            } catch (IllegalArgumentException e) {
                assertTrue(false);
            }
            assertEquals(null, proxy.getConf().getRangeSplit());
        }
        
        // 有split，但是值为空的情况
        // 输入：split = []
        // 期望：程序解析正常，且split的size为0
        {
            List<PrimaryKeyValue> rangeSplit = new ArrayList<PrimaryKeyValue>();
            readerConf.getConf().setRangeSplit(rangeSplit);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(true);
            } catch (IllegalArgumentException e) {
                assertTrue(false);
            }
            assertEquals(0, proxy.getConf().getRangeSplit().size());
        }
    }

    /**
     * maxRetryTime检查默认参数
     * @throws Exception
     */
    @Test
    public void testCheckDefaultParam_error_retry_limit() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // 测试默认值是否生效
        // 输入：
        // 期望：获取到默认值18
        {
            Configuration p = Configuration.from(readerConf.toString());
            proxy.init(p);
            assertEquals(18, proxy.getConf().getRetry());
        }

        // 测试配置的值是否生效
        // maxRetryTime : 100
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[],"
                            +    "\"end\":[]"
                            + "},"
                            + "\"maxRetryTime\": 100,"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            proxy.init(p);
            assertEquals(100, proxy.getConf().getRetry());
        }
    }

    /**
     * retrySleepInMillionSecond检查默认参数
     * @throws Exception
     */
    @Test
    public void testCheckDefaultParam_error_retry_sleep_in_millin_second() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // 测试默认值是否生效
        // 输入：
        // 期望：获取到默认值为100
        {
            Configuration p = Configuration.from(readerConf.toString());
            proxy.init(p);
            assertEquals(100, proxy.getConf().getSleepInMilliSecond());
        }
        
        // 测试配置的值是否生效
        // retrySleepInMillionSecond : 555
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[],"
                            +    "\"end\":[]"
                            + "},"
                            + "\"retrySleepInMillionSecond\": 555,"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            proxy.init(p);
            assertEquals(555, proxy.getConf().getSleepInMilliSecond());
        }
    }
}
