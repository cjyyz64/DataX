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
import com.alibaba.datax.plugin.reader.otsreader.utils.OtsModelUtils;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;

public class OtsModelUtilsUnittest {
    @Test
    public void testParseOTSColumn() {
        // 正常
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "string");
            item.put("value", "bigbang");
            OTSColumn col = OtsModelUtils.parseOTSColumn(item);
            assertEquals(OTSColumn.OTSColumnType.CONST, col.getType());
            assertEquals("bigbang", col.getValue().asString());
        }
        // 异常 1
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "strin");
            item.put("value", "bigbang");
            try {
                OtsModelUtils.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 2
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "integer");
            item.put("valu", "bigbang");
            try {
                OtsModelUtils.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 3
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "integer");
            item.put("", "bigbang");
            try {
                OtsModelUtils.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 4
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("", "integer");
            item.put("", "bigbang");
            try {
                OtsModelUtils.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 5
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("type", "");
            item.put("value", "bigbang");
            try {
                OtsModelUtils.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 异常 6
        {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            try {
                OtsModelUtils.parseOTSColumn(item);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
    }
    @Test
    public void testParseOTSColumnList() throws Exception {
        // 正常逻辑
        {
            List<Object> items = new ArrayList<Object>();
            items.add("col1");
            items.add("col2");
            items.add("col3");
            items.add("col4");
            List<OTSColumn> columns = OtsModelUtils.parseOTSColumnList(items);
            assertEquals(4, columns.size());
            assertEquals("col1", columns.get(0).getValue().asString());
            assertEquals("col2", columns.get(1).getValue().asString());
            assertEquals("col3", columns.get(2).getValue().asString());
            assertEquals("col4", columns.get(3).getValue().asString());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(0).getType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(1).getType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(2).getType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(3).getType());
        }
        // 测试 混合场景
        {
            Map<String, Object> strValue = new LinkedHashMap<String, Object>(); 
            strValue.put(OTSConst.TYPE, OTSConst.COLUMN_STRING);
            strValue.put(OTSConst.VALUE, "willan北京");

            Map<String, Object> intValue = new LinkedHashMap<String, Object>(); 
            intValue.put(OTSConst.TYPE, OTSConst.COLUMN_INTEGER);
            intValue.put(OTSConst.VALUE, "10923");

            Map<String, Object> doubleValue = new LinkedHashMap<String, Object>(); 
            doubleValue.put(OTSConst.TYPE, OTSConst.COLUMN_DOUBLE);
            doubleValue.put(OTSConst.VALUE, "10923");

            Map<String, Object> boolValue = new LinkedHashMap<String, Object>(); 
            boolValue.put(OTSConst.TYPE, OTSConst.COLUMN_BOOLEAN);
            boolValue.put(OTSConst.VALUE, "True");

            Person p = new Person();
            p.setName("李四");
            p.setAge(111);
            p.setHeight(120.00);
            p.setMale(false);

            Map<String, Object> binValue = new LinkedHashMap<String, Object>(); 
            binValue.put(OTSConst.TYPE, OTSConst.COLUMN_BINARY);
            binValue.put(OTSConst.VALUE, new String(Base64.encodeBase64(Person.toByte(p))));

            List<Object> items = new ArrayList<Object>();
            items.add("col1");
            items.add(strValue);
            items.add("col2");
            items.add(intValue);
            items.add("col2");
            items.add(doubleValue);
            items.add(boolValue);
            items.add(binValue);
            items.add(intValue);

            List<OTSColumn> columns = OtsModelUtils.parseOTSColumnList(items);
            assertEquals(9, columns.size());
            assertEquals("col1", columns.get(0).getValue().asString());
            assertEquals("willan北京", columns.get(1).getValue().asString());
            assertEquals("col2", columns.get(2).getValue().asString());
            assertEquals(10923, columns.get(3).getValue().asLong());
            assertEquals("col2", columns.get(4).getValue().asString());
            assertEquals(true, columns.get(5).getValue().asDouble() == 10923.0);
            assertEquals(true, columns.get(6).getValue().asBoolean());
            assertEquals(10923, columns.get(8).getValue().asLong());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(0).getType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(1).getType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(2).getType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(3).getType());
            assertEquals(OTSColumn.OTSColumnType.NORMAL, columns.get(4).getType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(5).getType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(6).getType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(7).getType());
            assertEquals(OTSColumn.OTSColumnType.CONST, columns.get(8).getType());

            Person newP = Person.toPerson(columns.get(7).getValue().asBinary());

            assertEquals("李四", newP.getName());
            assertEquals(111, newP.getAge());
            assertEquals(true, newP.getHeight() == 120.0);
            assertEquals(false, newP.isMale());

        }
        // 测试非法列名 
        {
            List<Object> items = new ArrayList<Object>();
            items.add(2999);
            try {
                OtsModelUtils.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        {
            List<Object> items = new ArrayList<Object>();
            items.add(true);
            try {
                OtsModelUtils.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        {
            List<Object> items = new ArrayList<Object>();
            items.add(12.322);
            try {
                OtsModelUtils.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        // 测试非法常量列
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.COLUMN_INTEGER);
            value.put(OTSConst.VALUE, 1222);

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                OtsModelUtils.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.COLUMN_DOUBLE);
            value.put(OTSConst.VALUE, 0.0);

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                OtsModelUtils.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.COLUMN_STRING);
            value.put(OTSConst.VALUE, true);

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                OtsModelUtils.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.COLUMN_BOOLEAN);
            value.put(OTSConst.VALUE, true);

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                OtsModelUtils.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
        {
            Map<String, Object> value = new LinkedHashMap<String, Object>();
            value.put(OTSConst.TYPE, OTSConst.COLUMN_BINARY);
            value.put(OTSConst.VALUE, "xxxxxxxxxxxx".getBytes());

            List<Object> items = new ArrayList<Object>();
            items.add(value);
            try {
                OtsModelUtils.parseOTSColumnList(items);
                assertTrue(false);
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            }
        }
    }
    @Test
    public void testParseColumnValue() throws Exception {
        // 正常逻辑解析
        {
            ColumnValue v1 = OtsModelUtils.parseColumnValue("String", "hello");
            assertEquals(ColumnType.STRING, v1.getType());
            assertEquals("hello", v1.asString());

            ColumnValue v2 = OtsModelUtils.parseColumnValue("integer", "1234");
            assertEquals(ColumnType.INTEGER, v2.getType());
            assertEquals(1234, v2.asLong());

            ColumnValue v3 = OtsModelUtils.parseColumnValue("doublE", "6442.12");
            assertEquals(ColumnType.DOUBLE, v3.getType());
            assertEquals(true, v3.asDouble() == 6442.12);

            ColumnValue v4 = OtsModelUtils.parseColumnValue("boolean", "true");
            assertEquals(ColumnType.BOOLEAN, v4.getType());
            assertEquals(true, v4.asBoolean());

            ColumnValue v5 = OtsModelUtils.parseColumnValue("BOOLEAN", "false");
            assertEquals(ColumnType.BOOLEAN, v5.getType());
            assertEquals(false, v5.asBoolean());

            String value = "world中国14为53495291‘，。，了；rwer";
            String encodeStr = Base64.encodeBase64String(value.getBytes());
            ColumnValue v6 = OtsModelUtils.parseColumnValue("BInary", encodeStr);
            assertEquals(ColumnType.BINARY, v6.getType());

            assertEquals(value, new String(v6.asBinary()));
        }

        // String边界条件
        {
            ColumnValue v1 = OtsModelUtils.parseColumnValue("String", "");
            assertEquals(ColumnType.STRING, v1.getType());
            assertEquals("", v1.asString());


            ColumnValue v2 = OtsModelUtils.parseColumnValue("String", "\0");
            assertEquals(ColumnType.STRING, v2.getType());
            assertEquals("\0", v2.asString());

            String ss = "另外，UT和FT是不一样的，也许UT的某个分支已经cover了我们所罗列的测试点"
                    + "，但是很有必要从黑盒的角度再把那个测试点重新审视一遍，因为FT的角度能衍生出"
                    + "更多的case。更重要的是，一个场景，用户使用正确的方式只有一种，但是使用错误"
                    + "的方式千奇百怪；一个系统的强大不仅仅是因为能提供有效的功能，更重要的是各种"
                    + "处理异常场景的能力。总是，测试场景构造要全面，忘记你的代码是怎么写的，把自己"
                    + "想成用户，而且是那种很笨的用户才行";
            ColumnValue v3 = OtsModelUtils.parseColumnValue("String", ss);
            assertEquals(ColumnType.STRING, v3.getType());
            assertEquals(ss, v3.asString());
        }

        // Integer边界
        {
            ColumnValue v1 = OtsModelUtils.parseColumnValue("integer", String.format("%d", Long.MIN_VALUE));
            assertEquals(ColumnType.INTEGER, v1.getType());
            assertEquals(Long.MIN_VALUE, v1.asLong());

            ColumnValue v2 = OtsModelUtils.parseColumnValue("integer", String.format("%d", Long.MAX_VALUE));
            assertEquals(ColumnType.INTEGER, v2.getType());
            assertEquals(Long.MAX_VALUE, v2.asLong());

            ColumnValue v3 = OtsModelUtils.parseColumnValue("integer", "010");
            assertEquals(ColumnType.INTEGER, v3.getType());
            assertEquals(10, v3.asLong());

            try {
                OtsModelUtils.parseColumnValue("integer", "xx010");
                assertTrue(false);
            } catch (NumberFormatException e) {
                assertTrue(true);
            }
        }

        // Double边界
        {
            ColumnValue v1 = OtsModelUtils.parseColumnValue("doUBLE", String.format("%f", Double.MIN_VALUE));
            assertEquals(ColumnType.DOUBLE, v1.getType());
            assertEquals("0.000000", String.format("%f", v1.asDouble()));

            ColumnValue v2 = OtsModelUtils.parseColumnValue("DOUBLE", String.format("%f", Double.MAX_VALUE));
            assertEquals(ColumnType.DOUBLE, v2.getType());
            assertTrue(Double.MAX_VALUE == v2.asDouble());

            ColumnValue v3 = OtsModelUtils.parseColumnValue("DOUBLE", "010.00");
            assertEquals(ColumnType.DOUBLE, v3.getType());
            assertTrue(10.0 == v3.asDouble());

            try {
                OtsModelUtils.parseColumnValue("DOUBLE", "xx010.00");
                assertTrue(false);
            } catch (NumberFormatException e) {
                assertTrue(true);
            }
        }
        // Boolean边界
        {
            ColumnValue v1 = OtsModelUtils.parseColumnValue("boolean", "True");
            assertEquals(ColumnType.BOOLEAN, v1.getType());
            assertEquals(true, v1.asBoolean());

            ColumnValue v2 = OtsModelUtils.parseColumnValue("boolean", "TRue");
            assertEquals(ColumnType.BOOLEAN, v2.getType());
            assertEquals(true, v2.asBoolean());

            ColumnValue v3 = OtsModelUtils.parseColumnValue("boolean", "TRUE");
            assertEquals(ColumnType.BOOLEAN, v3.getType());
            assertEquals(true, v3.asBoolean());

            ColumnValue v4 = OtsModelUtils.parseColumnValue("boolean", "False");
            assertEquals(ColumnType.BOOLEAN, v4.getType());
            assertEquals(false, v4.asBoolean());

            ColumnValue v5 = OtsModelUtils.parseColumnValue("boolean", "FalsE");
            assertEquals(ColumnType.BOOLEAN, v5.getType());
            assertEquals(false, v5.asBoolean());

            ColumnValue v6 = OtsModelUtils.parseColumnValue("boolean", "FALSE");
            assertEquals(ColumnType.BOOLEAN, v6.getType());
            assertEquals(false, v6.asBoolean());

            ColumnValue v7 = OtsModelUtils.parseColumnValue("boolean", "FALS=E---");
            assertEquals(ColumnType.BOOLEAN, v7.getType());
            assertEquals(false, v7.asBoolean());

            ColumnValue v8 = OtsModelUtils.parseColumnValue("boolean", "ssdfstrue---");
            assertEquals(ColumnType.BOOLEAN, v8.getType());
            assertEquals(false, v8.asBoolean());
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

            ColumnValue v1 = OtsModelUtils.parseColumnValue("binary", encodeStr);
            assertEquals(ColumnType.BINARY, v1.getType());
            assertEquals(ss, new String(v1.asBinary()));

            Person p = new Person();
            p.setName("张三");
            p.setAge(320);
            p.setHeight(180.00);
            p.setMale(true);

            encodeStr = Base64.encodeBase64String(Person.toByte(p));

            ColumnValue v2 = OtsModelUtils.parseColumnValue("binary", encodeStr);
            assertEquals(ColumnType.BINARY, v2.getType());

            Person newP = Person.toPerson(v2.asBinary());
            assertEquals("张三", newP.getName());
            assertEquals(320, newP.getAge());
            assertEquals(true, newP.getHeight() == 180.0);
            assertEquals(true, newP.isMale());
        }
    }
}
