package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.*;

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
public class OTSReaderProxyParamParseFunctiontest {
    private static String tableName = "ots_reader_proxy_param_parse_functiontest";
    private static Configuration p = Utils.loadConf();
    
    private ReaderConf readerConf = null;
    
    //@BeforeClass
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
    }

    /**
     * 基础功能测试，测试解析Json的正确性
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
        assertEquals(12, proxy.getConf().getRetry());
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
     * @throws Exception
     */
    @Test
    public void testCheckParam_accesskey() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
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
        // 如 : []
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
        
        // 非法的column name
        // 如 : "somevalue"
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":\"somevalue\"," //point
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
                assertEquals("The param  'column' is not a json array.", e.getMessage());
            }
        }
        
        // 非法的column name
        // 如 : [{"name": "col"}, "col1"]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\": \"col\"}, \"col1\"]," //point
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
                assertEquals("Can not parse Object to 'OTSColumn', item of list is not a map.", e.getMessage());
            }
        }

        // 非字符串的列名
        // 如 : [{\"name\":1000}]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":1000}]," //point
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
                assertEquals("Can not parse map to 'OTSColumn', the value is not a string.", e.getMessage());
            }
        }

        // 常量列中类型不对
        // 如 : [{\"type\":\"INTXXX\",\"value\":\"100\"}]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"type\":\"INTXXX\",\"value\":\"100\"}]," //point
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
                assertEquals("Can not parse map to 'OTSColumn', input type:INTXXX, value:100.", e.getMessage());
            }
        }

        // 常量列中值不对
        // 如 : [{\"type\":\"INT\",\"value\":100}]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"type\":\"INT\",\"value\":100}]," //point
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
                assertEquals("Can not parse map to 'OTSColumn', the value is not a string.", e.getMessage());
            }
        }
        
        // 常量列中只有type，没有value
        // 如 : [{\"type\":\"INT\"}]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"type\":\"INT\"}]," //point
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
                assertEquals("Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'. ", e.getMessage());
            }
        }
        // 常量列中只有value，没有type
        // 如 : [{\"type\":\"INT\"}]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"value\":\"11111\"}]," //point
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
                assertEquals("Can not parse map to 'OTSColumn', valid format: '{\"name\":\"\"}' or '{\"type\":\"\", \"value\":\"\"}'. ", e.getMessage());
            }
        }
        // 只有常量列的情况
        // 如 : [{\"type\":\"int\", \"value\":\"11111\"}]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"type\":\"int\", \"value\":\"11111\"}]," //point
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
                assertEquals("Invalid 'column', 'column' should include at least one or more Normal Column.", e.getMessage());
            }
        }
    }

    /**
     * 测试Range为空的情况
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
        // 测试begin和end类型不一致
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"int\", \"value\":\"900\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[{\"type\":\"string\", \"value\":\"hello\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Input range type not match primary key. Input type:INTEGER, Primary Key Type:STRING, Index:0", e.getMessage());
            }
        }
        // 测试split和begin，end类型不一致
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[],"
                            +    "\"end\":[],"
                            +    "\"split\":[{\"type\":\"int\", \"value\":\"1111\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Input type of 'range-split' not match partition key. Item of 'range-split' type:INTEGER, Partition type:STRING", e.getMessage());
            }
        }
        
        // Begin 为空，End不为空
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[],"
                            +    "\"end\":[{\"type\":\"string\", \"value\":\"hello\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"split\":[]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Input size of values not equal size of primary key. input size:0, primary key size:4 .", e.getMessage());
            }
        }
        // End 为空，Begin不为空
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"string\", \"value\":\"hello\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[],"
                            +    "\"split\":[]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Input size of values not equal size of primary key. input size:0, primary key size:4 .", e.getMessage());
            }
        }
    }
    
    /**
     * range begin 参数合法性的检查
     * @throws Exception
     */
    @Test
    public void testCheckParam_range_begin() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // range begin 不存在的检查
        {
            readerConf.getConf().setRangeBegin(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'begin' is not exist.", e.getMessage());
            }
        }
        // 错误的类型
        // 如：{\"type\":\"intt\", \"value\":\"900\"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"intt\", \"value\":\"900\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Not supprot parsing type: intt for PrimaryKeyValue.", e.getMessage());
            }
        }
        
        // 传入非法的PartitionKey类型
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"double\", \"value\":\"100.0\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Not supprot parsing type: double for PrimaryKeyValue.", e.getMessage());
            }
        }
        // 错误的Map，如：{"invalid":"bug", "type":"INF_MIN","value":""}, 预期不会报错
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"invalid\":\"bug\", \"type\":\"INF_MIN\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[{\"type\":\"inf_mAX\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(true);
            } catch (IllegalArgumentException e) {
                assertTrue(false);
            }
        }
        // 不存在的非法type， 如：{"type":"INVALID", "value":"1"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"INVALID\", \"value\":\"1\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Not supprot parsing type: INVALID for PrimaryKeyValue.", e.getMessage());
            }
        }
        // INF类型带值，如：{"type":"INF_MAX","value":"some"}，预期不会报错
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"INF_MIN\", \"value\":\"some\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[{\"type\":\"inf_mAX\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(true);
            } catch (IllegalArgumentException e) {
                assertTrue(false);
            }
        }
    }
    
    /**
     * range end 参数合法性的检查
     * @throws Exception
     */
    @Test
    public void testCheckParam_range_end() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // range end 不存在的情况
        {
            readerConf.getConf().setRangeEnd(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'end' is not exist.", e.getMessage());
            }
        }
    }

    /**
     * range split 参数合法性的检查
     * @throws Exception
     */
    @Test
    public void testCheckParam_range_split() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // 没有split的情况
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
        
        // split 的边界和begin，有重叠,如：{\"type\":\"string\", \"value\":\"0\"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"string\", \"value\":\"0\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":  [{\"type\":\"inf_MAX\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"split\":[{\"type\":\"string\", \"value\":\"0\"},{\"type\":\"string\", \"value\":\"10\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The item of 'range-split' is not within scope of 'range-begin' and 'range-end'.", e.getMessage());
            }
        }
        // split 的边界和end，有重叠,如：{\"type\":\"string\", \"value\":\"中国\"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"string\", \"value\":\"0\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[{\"type\":\"string\", \"value\":\"中国\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"split\":[{\"type\":\"string\", \"value\":\"10\"}, {\"type\":\"string\", \"value\":\"20\"},{\"type\":\"string\", \"value\":\"中国\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The item of 'range-split' is not within scope of 'range-begin' and 'range-end'.", e.getMessage());
            }
        }
        // split 的所有点和begin，end没有交集
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"string\", \"value\":\"a\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":[{\"type\":\"string\", \"value\":\"z\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"split\":[{\"type\":\"string\", \"value\":\"0\"}, {\"type\":\"string\", \"value\":\"20\"},{\"type\":\"string\", \"value\":\"30\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The item of 'range-split' is not within scope of 'range-begin' and 'range-end'.", e.getMessage());
            }
        }
        // split 的部分点和begin，end有交集，左边交集
        //    [55~77)
        // [00~69]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"string\", \"value\":\"55\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":  [{\"type\":\"string\", \"value\":\"77\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"split\":[{\"type\":\"string\", \"value\":\"00\"}, {\"type\":\"string\", \"value\":\"66\"},{\"type\":\"string\", \"value\":\"69\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The item of 'range-split' is not within scope of 'range-begin' and 'range-end'.", e.getMessage());
            }
        }
        // split 的部分点和begin，end有交集，右边交集
        // [55~77)
        //    [69~99]
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"string\", \"value\":\"55\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":  [{\"type\":\"string\", \"value\":\"77\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"split\":[{\"type\":\"string\", \"value\":\"69\"},{\"type\":\"string\", \"value\":\"99\"}]"
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The item of 'range-split' is not within scope of 'range-begin' and 'range-end'.", e.getMessage());
            }
        }
        // split 中的点类型不一致,如：{\"type\":\"string\", \"value\":\"56\"},{\"type\":\"int\", \"value\":\"66\"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"string\", \"value\":\"55\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":  [{\"type\":\"string\", \"value\":\"77\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"split\":[{\"type\":\"string\", \"value\":\"56\"},{\"type\":\"int\", \"value\":\"66\"}]" //point
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Not same column type, column1:STRING, column2:INTEGER", e.getMessage());
            }
        }
        // split 中的点有重复点, 如：{\"type\":\"string\", \"value\":\"66\"},{\"type\":\"string\", \"value\":\"66\"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[{\"type\":\"string\", \"value\":\"55\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"end\":  [{\"type\":\"string\", \"value\":\"77\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"},{\"type\":\"inf_min\", \"value\":\"\"}],"
                            +    "\"split\":[{\"type\":\"string\", \"value\":\"66\"},{\"type\":\"String\", \"value\":\"66\"}]" //point
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Multi same column in 'range-split'.", e.getMessage());
            }
        }
        // split 中的点格式不对, {\"name\":\"string\", \"value\":\"66\"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[],"
                            +    "\"end\":  [],"
                            +    "\"split\":[{\"name\":\"string\", \"value\":\"66\"}]" //point
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The map must include 'type' and 'value'.", e.getMessage());
            }
        }
        // split 中的点格式不对, {\"type\":\"int\", \"value\":\"\"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[],"
                            +    "\"end\":  [],"
                            +    "\"split\":[{\"type\":\"int\", \"value\":\"\"}]" //point
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Can not parse the value '' to Int.", e.getMessage());
            }
        }
        // split 中的点格式不对, {\"type\":\"int\", \"value\":\"hello\"}
        {
            String json = 
                    "{\"accessId\":\""+ p.getString("accessid") +"\","
                            + "\"accessKey\":\""+ p.getString("accesskey") +"\","
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\","
                            + "\"instanceName\":\""+ p.getString("instance-name") +"\","
                            + "\"column\":[{\"name\":\"xxxx\"}],"
                            + "\"range\":{"
                            +    "\"begin\":[],"
                            +    "\"end\":  [],"
                            +    "\"split\":[{\"type\":\"int\", \"value\":\"hello\"}]" //point
                            + "},"
                            + "\"table\":\""+ tableName +"\"}";
            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Can not parse the value 'hello' to Int.", e.getMessage());
            }
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
        {
            Configuration p = Configuration.from(readerConf.toString());
            proxy.init(p);
            assertEquals(12, proxy.getConf().getRetry());
        }
        // 测试配置的值是否生效
        {
            readerConf.getConf().setRetry(100);
            Configuration p = Configuration.from(readerConf.toString());
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
        {
            Configuration p = Configuration.from(readerConf.toString());
            proxy.init(p);
            assertEquals(100, proxy.getConf().getSleepInMilliSecond());
        }
        // 测试配置的值是否生效
        {
            readerConf.getConf().setSleepInMilliSecond(2999);
            Configuration p = Configuration.from(readerConf.toString());
            proxy.init(p);
            assertEquals(2999, proxy.getConf().getSleepInMilliSecond());
        }
    }
}
