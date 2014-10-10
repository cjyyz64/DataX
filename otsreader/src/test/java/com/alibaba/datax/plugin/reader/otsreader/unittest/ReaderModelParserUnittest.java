package com.alibaba.datax.plugin.reader.otsreader.unittest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import com.alibaba.datax.plugin.reader.otsreader.common.Person;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConst;
import com.alibaba.datax.plugin.reader.otsreader.utils.ReaderModelParser;
import com.aliyun.openservices.ots.model.ColumnType;

public class ReaderModelParserUnittest {
    /**
     * 测试解析Column是否正常
     */
    @Test
    public void testParseOTSColumn() {
        // 正常
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "string");
            item.put("value", "bigbang");
            OTSColumn col = ReaderModelParser.parseOTSColumn(item);
            assertEquals(OTSColumn.OTSColumnType.CONST, col.getColumnType());
            assertEquals("bigbang", col.getValue().asString());
        }
        // 异常 1，错误的type ‘strin’
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "strin");
            item.put("value", "bigbang");
            try {
                ReaderModelParser.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 2，错误的value ‘bigbang’
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "int");
            item.put("valu", "bigbang");
            try {
                ReaderModelParser.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 3，缺少value
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "int");
            item.put("", "bigbang");
            try {
                ReaderModelParser.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 4，缺少type和value
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("", "int");
            item.put("", "bigbang");
            try {
                ReaderModelParser.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 5，type和value类型不匹配
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "");
            item.put("value", "bigbang");
            try {
                ReaderModelParser.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 6， 缺少type和value
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            try {
                ReaderModelParser.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
    }
    
    /**
     * 测试在混合Column的情况下， Column解析的正确性
     * @throws Exception
     */
    @Test
    public void testParseOTSColumnList() throws Exception {
        // 正常逻辑
        {
            Map<String, Object> col1 = new LinkedHashMap<String, Object>(); 
            col1.put(OTSConst.NAME, "col1");
            
            Map<String, Object> col2 = new LinkedHashMap<String, Object>(); 
            col2.put(OTSConst.NAME, "col2");
            
            Map<String, Object> col3 = new LinkedHashMap<String, Object>(); 
            col3.put(OTSConst.NAME, "col3");
            
            Map<String, Object> col4 = new LinkedHashMap<String, Object>(); 
            col4.put(OTSConst.NAME, "col4");
            
            List<Object> items = new ArrayList<Object>();
            items.add(col1);
            items.add(col2);
            items.add(col3);
            items.add(col4);
            
            List<OTSColumn> columns = ReaderModelParser.parseOTSColumnList(items);
            
            assertEquals(4, columns.size());
            assertEquals("col1", columns.get(0).getName());
            assertEquals("col2", columns.get(1).getName());
            assertEquals("col3", columns.get(2).getName());
            assertEquals("col4", columns.get(3).getName());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(0).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(1).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(2).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(3).getColumnType());
        }
        // 测试 混合场景
        {
            Map<String, Object> strValue = new LinkedHashMap<String, Object>(); 
            strValue.put(OTSConst.TYPE, OTSConst.TYPE_STRING);
            strValue.put(OTSConst.VALUE, "willan北京");

            Map<String, Object> intValue = new LinkedHashMap<String, Object>(); 
            intValue.put(OTSConst.TYPE, OTSConst.TYPE_INTEGER);
            intValue.put(OTSConst.VALUE, "10923");

            Map<String, Object> doubleValue = new LinkedHashMap<String, Object>(); 
            doubleValue.put(OTSConst.TYPE, OTSConst.TYPE_DOUBLE);
            doubleValue.put(OTSConst.VALUE, "10923");

            Map<String, Object> boolValue = new LinkedHashMap<String, Object>(); 
            boolValue.put(OTSConst.TYPE, OTSConst.TYPE_BOOLEAN);
            boolValue.put(OTSConst.VALUE, "True");
            
            Map<String, Object> col1 = new LinkedHashMap<String, Object>(); 
            col1.put(OTSConst.NAME, "col1");
            
            Map<String, Object> col2 = new LinkedHashMap<String, Object>(); 
            col2.put(OTSConst.NAME, "col2");

            Person p = new Person();
            p.setName("李四");
            p.setAge(111);
            p.setHeight(120.00);
            p.setMale(false);

            Map<String, Object> binValue = new LinkedHashMap<String, Object>(); 
            binValue.put(OTSConst.TYPE, OTSConst.TYPE_BINARY);
            binValue.put(OTSConst.VALUE, new String(Base64.encodeBase64(Person.toByte(p))));

            List<Object> items = new ArrayList<Object>();
            items.add(col1);
            items.add(strValue);
            items.add(col2);
            items.add(intValue);
            items.add(col2);
            items.add(doubleValue);
            items.add(boolValue);
            items.add(binValue);
            items.add(intValue);

            List<OTSColumn> columns = ReaderModelParser.parseOTSColumnList(items);
            assertEquals(9, columns.size());
            assertEquals("col1", columns.get(0).getName());
            assertEquals("willan北京", columns.get(1).getValue().asString());
            assertEquals("col2", columns.get(2).getName());
            assertEquals(10923, columns.get(3).getValue().asLong().longValue());
            assertEquals("col2", columns.get(4).getName());
            assertEquals(true, columns.get(5).getValue().asDouble() == 10923.0);
            assertEquals(true, columns.get(6).getValue().asBoolean());
            assertEquals(10923, columns.get(8).getValue().asLong().longValue());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(0).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(1).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(2).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(3).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(4).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(5).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(6).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(7).getColumnType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(8).getColumnType());

            Person newP = Person.toPerson(columns.get(7).getValue().asBytes());

            assertEquals("李四", newP.getName());
            assertEquals(111, newP.getAge());
            assertEquals(true, newP.getHeight() == 120.0);
            assertEquals(false, newP.isMale());

        }
        // 测试非法列名 ， 整形列名
        {
            List<Object> items = new ArrayList<Object>();
            items.add(2999);
            try {
                ReaderModelParser.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 测试非法列名 ， 布尔列名
        {
            List<Object> items = new ArrayList<Object>();
            items.add(true);
            try {
                ReaderModelParser.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 测试非法列名 ， 浮点列名
        {
            List<Object> items = new ArrayList<Object>();
            items.add(12.322);
            try {
                ReaderModelParser.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 测试非法常量列，系统不支持value的值为整形
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.TYPE_INTEGER);
            value.put(OTSConst.VALUE, 1222);

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                ReaderModelParser.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 测试非法常量列，系统不支持value的值为浮点型
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.TYPE_DOUBLE);
            value.put(OTSConst.VALUE, 0.0);

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                ReaderModelParser.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 测试非法常量列，系统不支持value的值为布尔型
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.TYPE_STRING);
            value.put(OTSConst.VALUE, true);

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                ReaderModelParser.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.TYPE_BOOLEAN);
            value.put(OTSConst.VALUE, true);

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                ReaderModelParser.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 测试非法常量列，系统不支持value的值为二进制
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.TYPE_BINARY);
            value.put(OTSConst.VALUE, "xxxxxxxxxxxx".getBytes());

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                ReaderModelParser.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
    }
    
    /**
     * 测试解析ColumnValue的正确性
     * @throws Exception
     */
    @Test
    public void testParseConstColumn() throws Exception {
        // 正常逻辑解析
        {
            OTSColumn v1 = ReaderModelParser.parseConstColumn("String", "hello");
            assertEquals(ColumnType.STRING, v1.getValueType());
            assertEquals("hello", v1.getValue().asString());

            OTSColumn v2 = ReaderModelParser.parseConstColumn("int", "1234");
            assertEquals(ColumnType.INTEGER, v2.getValueType());
            assertEquals(1234, v2.getValue().asLong().longValue());

            OTSColumn v3 = ReaderModelParser.parseConstColumn("doublE", "6442.12");
            assertEquals(ColumnType.DOUBLE, v3.getValueType());
            assertEquals(true, v3.getValue().asDouble() == 6442.12);

            OTSColumn v4 = ReaderModelParser.parseConstColumn("bool", "true");
            assertEquals(ColumnType.BOOLEAN, v4.getValueType());
            assertEquals(true, v4.getValue().asBoolean());

            OTSColumn v5 = ReaderModelParser.parseConstColumn("BOOL", "false");
            assertEquals(ColumnType.BOOLEAN, v5.getValueType());
            assertEquals(false, v5.getValue().asBoolean());

            String value = "world中国14为53495291‘，。，了；rwer";
            String encodeStr = Base64.encodeBase64String(value.getBytes());
            OTSColumn v6 = ReaderModelParser.parseConstColumn("BInary", encodeStr);
            assertEquals(ColumnType.BINARY, v6.getValueType());

            assertEquals(value, new String(v6.getValue().asBytes()));
        }

        // String边界条件
        {
            OTSColumn v1 = ReaderModelParser.parseConstColumn("String", "");
            assertEquals(ColumnType.STRING, v1.getValueType());
            assertEquals("", v1.getValue().asString());


            OTSColumn v2 = ReaderModelParser.parseConstColumn("String", "\0");
            assertEquals(ColumnType.STRING, v2.getValueType());
            assertEquals("\0", v2.getValue().asString());

            String ss = "另外，UT和FT是不一样的，也许UT的某个分支已经cover了我们所罗列的测试点"
                    + "，但是很有必要从黑盒的角度再把那个测试点重新审视一遍，因为FT的角度能衍生出"
                    + "更多的case。更重要的是，一个场景，用户使用正确的方式只有一种，但是使用错误"
                    + "的方式千奇百怪；一个系统的强大不仅仅是因为能提供有效的功能，更重要的是各种"
                    + "处理异常场景的能力。总是，测试场景构造要全面，忘记你的代码是怎么写的，把自己"
                    + "想成用户，而且是那种很笨的用户才行";
            OTSColumn v3 = ReaderModelParser.parseConstColumn("String", ss);
            assertEquals(ColumnType.STRING, v3.getValueType());
            assertEquals(ss, v3.getValue().asString());
        }

        // Integer边界
        {
            OTSColumn v1 = ReaderModelParser.parseConstColumn("int", String.format("%d", Long.MIN_VALUE));
            assertEquals(ColumnType.INTEGER, v1.getValueType());
            assertEquals(Long.MIN_VALUE, v1.getValue().asLong().longValue());

            OTSColumn v2 = ReaderModelParser.parseConstColumn("int", String.format("%d", Long.MAX_VALUE));
            assertEquals(ColumnType.INTEGER, v2.getValueType());
            assertEquals(Long.MAX_VALUE, v2.getValue().asLong().longValue());

            OTSColumn v3 = ReaderModelParser.parseConstColumn("int", "010");
            assertEquals(ColumnType.INTEGER, v3.getValueType());
            assertEquals(10, v3.getValue().asLong().longValue());

            try {
                ReaderModelParser.parseConstColumn("int", "xx010");
                assertTrue(false);
            } catch (NumberFormatException e) {
                assertTrue(true);
            }
        }

        // Double边界
        {
            OTSColumn v1 = ReaderModelParser.parseConstColumn("doUBLE", String.format("%f", Double.MIN_VALUE));
            assertEquals(ColumnType.DOUBLE, v1.getValueType());
            assertEquals("0.000000", String.format("%f", v1.getValue().asDouble()));

            OTSColumn v2 = ReaderModelParser.parseConstColumn("DOUBLE", String.format("%f", Double.MAX_VALUE));
            assertEquals(ColumnType.DOUBLE, v2.getValueType());
            assertTrue(Double.MAX_VALUE == v2.getValue().asDouble());

            OTSColumn v3 = ReaderModelParser.parseConstColumn("DOUBLE", "010.00");
            assertEquals(ColumnType.DOUBLE, v3.getValueType());
            assertTrue(10.0 == v3.getValue().asDouble());

            try {
                ReaderModelParser.parseConstColumn("DOUBLE", "xx010.00");
                assertTrue(false);
            } catch (NumberFormatException e) {
                assertTrue(true);
            }
        }
        // Boolean边界
        {
            OTSColumn v1 = ReaderModelParser.parseConstColumn("bool", "True");
            assertEquals(ColumnType.BOOLEAN, v1.getValueType());
            assertEquals(true, v1.getValue().asBoolean());

            OTSColumn v2 = ReaderModelParser.parseConstColumn("bool", "TRue");
            assertEquals(ColumnType.BOOLEAN, v2.getValueType());
            assertEquals(true, v2.getValue().asBoolean());

            OTSColumn v3 = ReaderModelParser.parseConstColumn("bool", "TRUE");
            assertEquals(ColumnType.BOOLEAN, v3.getValueType());
            assertEquals(true, v3.getValue().asBoolean());

            OTSColumn v4 = ReaderModelParser.parseConstColumn("bool", "False");
            assertEquals(ColumnType.BOOLEAN, v4.getValueType());
            assertEquals(false, v4.getValue().asBoolean());

            OTSColumn v5 = ReaderModelParser.parseConstColumn("bool", "FalsE");
            assertEquals(ColumnType.BOOLEAN, v5.getValueType());
            assertEquals(false, v5.getValue().asBoolean());

            OTSColumn v6 = ReaderModelParser.parseConstColumn("bool", "FALSE");
            assertEquals(ColumnType.BOOLEAN, v6.getValueType());
            assertEquals(false, v6.getValue().asBoolean());

            OTSColumn v7 = ReaderModelParser.parseConstColumn("bool", "FALS=E---");
            assertEquals(ColumnType.BOOLEAN, v7.getValueType());
            assertEquals(false, v7.getValue().asBoolean());

            OTSColumn v8 = ReaderModelParser.parseConstColumn("bool", "ssdfstrue---");
            assertEquals(ColumnType.BOOLEAN, v8.getValueType());
            assertEquals(false, v8.getValue().asBoolean());
        }
        // Binary边界
        {
            String ss = "另外，UT和FT是不一样的，也许UT的某个分支已经cover了我们所罗列的测试点"
                    + "，但是很有必要从黑盒的角度再把那个测试点重新审视一遍，因为FT的角度能衍生出"
                    + "更多的case。更重要的是，一个场景，用户使用正确的方式只有一种，但是使用错误"
                    + "的方式千奇百怪；一个系统的强大不仅仅是因为能提供有效的功能，更重要的是各种"
                    + "处理异常场景的能力。总是，测试场景构造要全面，忘记你的代码是怎么写的，把自己"
                    + "想成用户，而且是那种很笨的用户才行";

            String encodeStr = Base64.encodeBase64String(ss.getBytes());

            OTSColumn v1 = ReaderModelParser.parseConstColumn("binary", encodeStr);
            assertEquals(ColumnType.BINARY, v1.getValueType());
            assertEquals(ss, new String(v1.getValue().asBytes()));

            Person p = new Person();
            p.setName("张三");
            p.setAge(320);
            p.setHeight(180.00);
            p.setMale(true);

            encodeStr = Base64.encodeBase64String(Person.toByte(p));

            OTSColumn v2 = ReaderModelParser.parseConstColumn("binary", encodeStr);
            assertEquals(ColumnType.BINARY, v2.getValueType());

            Person newP = Person.toPerson(v2.getValue().asBytes());
            assertEquals("张三", newP.getName());
            assertEquals(320, newP.getAge());
            assertEquals(true, newP.getHeight() == 180.0);
            assertEquals(true, newP.isMale());
        }
    }
}
