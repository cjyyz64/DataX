package com.alibaba.datax.plugin.reader.otsreader.functiontest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.OtsReaderMasterProxy;
import com.alibaba.datax.plugin.reader.otsreader.common.Person;
import com.alibaba.datax.plugin.reader.otsreader.common.ReaderConf;
import com.alibaba.datax.plugin.reader.otsreader.common.Utils;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.TableMeta;

public class OTSReaderProxyParamParseFunctiontest {
    private String tableName = "ots_reader_proxy_param_parse_functiontest";
    private ReaderConf readerConf = null;
    private Configuration p = Utils.loadConf();

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
        columns.add(new OTSColumn(ColumnValue.fromString("col0"), OTSColumn.OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("col1"), OTSColumn.OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("col2"), OTSColumn.OTSColumnType.NORMAL));
        columns.add(new OTSColumn(ColumnValue.fromString("col2"), OTSColumn.OTSColumnType.NORMAL));

        columns.add(new OTSColumn(ColumnValue.fromString(""), OTSColumn.OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromString("测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^"), OTSColumn.OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromLong(100), OTSColumn.OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromDouble(1121111111111.0), OTSColumn.OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromBoolean(true), OTSColumn.OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromBoolean(false), OTSColumn.OTSColumnType.CONST));
        columns.add(new OTSColumn(ColumnValue.fromBinary(Person.toByte(person)), OTSColumn.OTSColumnType.CONST));

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

        conf.setRetry(100);
        conf.setSleepInMilliSecond(2999);

        readerConf.setConf(conf);
        
        {
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
    }

    // =========================================================================
    // 1.基础测试，测试解析参数正常，并符合预期
    // =========================================================================

    @Test
    public void testBaseInit() throws Exception {
        
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        Configuration p = Configuration.from(readerConf.toString());

        proxy.init(p);

        assertEquals(p.getString("endpoint"), proxy.getConf().getEndpoint());
        assertEquals(p.getString("accessid"), proxy.getConf().getAccessId());
        assertEquals(p.getString("accesskey"), proxy.getConf().getAccesskey());
        assertEquals(p.getString("instance-name"), proxy.getConf().getInstanceName());
        assertEquals(tableName, proxy.getConf().getTableName());
        assertEquals(100, proxy.getConf().getRetry());
        assertEquals(2999, proxy.getConf().getSleepInMilliSecond());
        assertEquals(11, proxy.getConf().getColumns().size());
        assertEquals(4, proxy.getConf().getRangeBegin().size());
        assertEquals(4, proxy.getConf().getRangeEnd().size());
        assertEquals(0, proxy.getConf().getRangeSplit().size());

        List<OTSColumn>columns = proxy.getConf().getColumns();
        OTSColumn col0 = columns.get(0);
        assertEquals(OTSColumn.OTSColumnType.NORMAL, col0.getType());
        assertEquals("col0", col0.getValue().asString());

        OTSColumn col1 = columns.get(1);
        assertEquals(OTSColumn.OTSColumnType.NORMAL, col1.getType());
        assertEquals("col1", col1.getValue().asString());

        OTSColumn col2 = columns.get(2);
        assertEquals(OTSColumn.OTSColumnType.NORMAL, col2.getType());
        assertEquals("col2", col2.getValue().asString());

        OTSColumn col3 = columns.get(3);
        assertEquals(OTSColumn.OTSColumnType.NORMAL, col3.getType());
        assertEquals("col2", col3.getValue().asString());

        OTSColumn col4 = columns.get(4);
        assertEquals(OTSColumn.OTSColumnType.CONST, col4.getType());
        assertEquals("", col4.getValue().asString());

        OTSColumn col5 = columns.get(5);
        assertEquals(OTSColumn.OTSColumnType.CONST, col5.getType());
        assertEquals("测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^", col5.getValue().asString());

        OTSColumn col6 = columns.get(6);
        assertEquals(OTSColumn.OTSColumnType.CONST, col6.getType());
        assertEquals(100, col6.getValue().asLong());

        OTSColumn col7 = columns.get(7);
        assertEquals(OTSColumn.OTSColumnType.CONST, col7.getType());
        assertEquals(true, 1121111111111.0 == col7.getValue().asDouble());

        OTSColumn col8 = columns.get(8);
        assertEquals(OTSColumn.OTSColumnType.CONST, col8.getType());
        assertEquals(true, col8.getValue().asBoolean());

        OTSColumn col9 = columns.get(9);
        assertEquals(OTSColumn.OTSColumnType.CONST, col9.getType());
        assertEquals(false, col9.getValue().asBoolean());

        OTSColumn col10 = columns.get(10);
        assertEquals(OTSColumn.OTSColumnType.CONST, col10.getType());

        Person newP = Person.toPerson(col10.getValue().asBinary());
        assertEquals("为硬音k，近似普通话轻声以外的g: cum,cīvis,facilis", newP.getName());
        assertEquals(Long.MAX_VALUE, newP.getAge());
        assertEquals(true, newP.getHeight() == 1111.0);
        assertEquals(false, newP.isMale());
    }

    // =========================================================================
    // 2.检查参数是否存在以及是否为空
    // endpoint,accessid,accesskey,instance-name,table,column,range-begin,
    // range-end,range-split
    // =========================================================================

    @Test
    public void testCheckParamNullAndEmpty_endpoint() throws Exception {
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

    @Test
    public void testCheckParamNullAndEmpty_accessid() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        {
            readerConf.getConf().setAccessId(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'accessid' is not exist.", e.getMessage());
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
                assertEquals("The param length of 'accessid' is zero.", e.getMessage());
            }
        }
    }

    @Test
    public void testCheckParamNullAndEmpty_accesskey() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        {
            readerConf.getConf().setAccesskey(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'accesskey' is not exist.", e.getMessage());
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
                assertEquals("The param length of 'accesskey' is zero.", e.getMessage());
            }
        }
    }

    @Test
    public void testCheckParamNullAndEmpty_instancename() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        {
            readerConf.getConf().setInstanceName(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'instance-name' is not exist.", e.getMessage());
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
                assertEquals("The param length of 'instance-name' is zero.", e.getMessage());
            }
        }
    }

    @Test
    public void testCheckParamNullAndEmpty_table() throws Exception {
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

    @Test
    public void testCheckParamNullAndEmpty_column() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
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
        // size == 0
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
    }

    @Test
    public void testCheckParamNullAndEmpty_range_begin() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        {
            readerConf.getConf().setRangeBegin(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'range-begin' is not exist.", e.getMessage());
            }
        }
        // size == 0
        {
            List<PrimaryKeyValue> columns = new ArrayList<PrimaryKeyValue>();
            readerConf.getConf().setRangeBegin(columns);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param  'range-begin' is empty.", e.getMessage());
            }
        }
    }

    @Test
    public void testCheckParamNullAndEmpty_range_end() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();
        // null
        {
            readerConf.getConf().setRangeEnd(null);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param 'range-end' is not exist.", e.getMessage());
            }
        }
        // size == 0
        {
            List<PrimaryKeyValue> columns = new ArrayList<PrimaryKeyValue>();
            readerConf.getConf().setRangeEnd(columns);
            Configuration p = Configuration.from(readerConf.toString());
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("The param  'range-end' is empty.", e.getMessage());
            }
        }
    }

    // =========================================================================
    // 3.检查参数的格式是否合法(range-begin,range-end,range-split)
    // =========================================================================

    @Test
    public void testCheckInvalidParam_column() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();

        // 非字符串的列名
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",1000,"
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"100\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_MAX\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGERR\",\"value\":\"2313443\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MAX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Unsupport parse.", e.getMessage());
            }
        }

        // 常量列中类型不对
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",\"col2\","
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGE\",\"value\":\"100\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MAX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Unsupport parse.", e.getMessage());
            }
        }

        // 常量列中值不对
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",\"col2\","
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"Pefect\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MAX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("For input string: \"Pefect\"", e.getMessage());
            }
        }
    }

    @Test
    public void testCheckInvalidParam_range_begin() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();

        // 错误的类型
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",\"col2\","
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"100\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MAX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Unsupport parse.", e.getMessage());
            }
        }
        // 类型正确，值不对
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",\"col2\","
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"100\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_MAX\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"hello\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MAX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (NumberFormatException e) {
                assertEquals("For input string: \"hello\"", e.getMessage());
            }
        }
    }

    @Test
    public void testCheckInvalidParam_range_end() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();

        // 错误的类型
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",\"col2\","
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"100\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_MAX\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGERR\",\"value\":\"2313443\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MAX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Unsupport parse.", e.getMessage());
            }
        }
        // 类型正确，值不对
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",\"col2\","
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"100\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_MAX\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"199\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"world\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MAX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (NumberFormatException e) {
                assertEquals("For input string: \"world\"", e.getMessage());
            }
        }
    }

    @Test
    public void testCheckInvalidParam_range_split() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();

        // 错误的类型
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",\"col2\","
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"100\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_MIN\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2313443\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"2313443\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertEquals("Unsupport parse.", e.getMessage());
            }
        }
        // 类型正确，值不对
        {
            String json = 
                    "{\"accessid\":\""+ p.getString("accessid") +"\",\"accesskey\":\""+ p.getString("accesskey") +"\","
                            + "\"column\":[\"col0\",\"col1\",\"col2\",\"col2\","
                            + "{\"type\":\"STRING\",\"value\":\"\"},"
                            + "{\"type\":\"STRING\",\"value\":"
                            + "\"测试切分功能正常，切分的范围符合预期!@!!$)(*&^%^\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"100\"},"
                            + "{\"type\":\"DOUBLE\",\"value\":\"1121111111111.000000\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"true\"},"
                            + "{\"type\":\"BOOLEAN\",\"value\":\"false\"},"
                            + "{\"type\":\"BINARY\",\"value\":\"rO0ABXNyADdjb20uYWxpYmFiYS5kYXRheC5"
                            + "wbHVnaW4ucmVhZGVyLm90c3JlYWRlci5jb21tb24u\r\nUGVyc29udpFe6B/Nvs0CAAR"
                            + "KAANhZ2VEAAZoZWlnaHRaAARtYWxlTAAEbmFtZXQAEkxqYXZhL2xh\r\nbmcvU3RyaW5"
                            + "nO3hwf/////////9AkVwAAAAAAAB0AEDkuLrnoazpn7Nr77yM6L+R5Ly85pmu6YCa\r\n"
                            + "6K+d6L275aOw5Lul5aSW55qEZzogY3VtLGPEq3ZpcyxmYWNpbGlz\r\n\"}],"
                            + "\"endpoint\":\""+ p.getString("endpoint") +"\",\"error-retry-limit\":100,"
                            + "\"error-retry-sleep-in-million-second\":2999,\"instance-name\":"
                            + "\""+ p.getString("instance-name") +"\",\"range-begin\":[{\"type\":\"INF_MAX\","
                            + "\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"199\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"range-end\":["
                            + "{\"type\":\"INF_MAX\",\"value\":\"\"},{\"type\":\"INF_MIN\",\"value\":\"\"},"
                            + "{\"type\":\"INTEGER\",\"value\":\"2999\"},{\"type\":\"STRING\",\"value\":\"中国\"}],"
                            + "\"range-split\":[{\"type\":\"INF_MAX\",\"value\":\"\"},"
                            + "{\"type\":\"INF_MIN\",\"value\":\"\"},{\"type\":\"INTEGER\",\"value\":\"xxxxx\"},"
                            + "{\"type\":\"STRING\",\"value\":\"中国\"}],\"table\":\""+ tableName +"\"}";

            Configuration p = Configuration.from(json);
            try {
                proxy.init(p);
                assertTrue(false);
            } catch (NumberFormatException e) {
                assertEquals("For input string: \"xxxxx\"", e.getMessage());
            }
        }
    }

    // =========================================================================
    // 4.检查默认参数（error-retry-limit，error-retry-sleep-in-million-second）
    // =========================================================================

    @Test
    public void testCheckDefaultParam_error_retry_limit() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();

        readerConf.getConf().setRetry(-1);
        Configuration p = Configuration.from(readerConf.toString());

        proxy.init(p);

        assertEquals(20, proxy.getConf().getRetry());
    }

    @Test
    public void testCheckDefaultParam_error_retry_sleep_in_millin_second() throws Exception {
        OtsReaderMasterProxy proxy = new OtsReaderMasterProxy();

        readerConf.getConf().setSleepInMilliSecond(-1);
        Configuration p = Configuration.from(readerConf.toString());

        proxy.init(p);

        assertEquals(50, proxy.getConf().getSleepInMilliSecond());
    }
}
