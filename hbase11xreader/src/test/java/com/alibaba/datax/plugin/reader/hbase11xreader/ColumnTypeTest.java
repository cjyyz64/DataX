package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.exception.DataXException;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;

/**
 * Created by shf on 16/7/20.
 *
 */
public class ColumnTypeTest {

    @Test
    public void testGetByTypeName() throws Exception {
        //正常类型
        String type = "boolean";
        ColumnType columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.BOOLEAN.equals(columnType));

        type = "short";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.SHORT.equals(columnType));

        type = "int";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.INT.equals(columnType));

        type = "long";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.LONG.equals(columnType));

        type = "float";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.FLOAT.equals(columnType));

        type = "double";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.DOUBLE.equals(columnType));

        type = "date";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.DATE.equals(columnType));


        type = "string";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.STRING.equals(columnType));

        type = "binarystring";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.BINARY_STRING.equals(columnType));
    }

    @Test
    public void testGetByTypeNameCase() throws Exception {
        //大写
        String type = "STRing";
        ColumnType columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.STRING.equals(columnType));

        type = "biNARYstring";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.BINARY_STRING.equals(columnType));
    }

    @Test
    public void testGetByTypeNameHaveBlank() throws Exception {
        //有空格
        String type = "  STRing  ";
        ColumnType columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.STRING.equals(columnType));

        type = "   biNARYstring";
        columnType = ColumnType.getByTypeName(type);
        assertTrue(ColumnType.BINARY_STRING.equals(columnType));
    }

    @Test
    public void testGetByTypeNameNotSupport() throws Exception {
        //异常测试
        String type = "  ";
        try{
            ColumnType.getByTypeName(type);
        }catch (Exception e){
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("Hbasereader 不支持该类型:  "));
        }

        type = "aaa";
        try{
             ColumnType.getByTypeName(type);
        }catch (Exception e){
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("Hbasereader 不支持该类型:aaa"));
        }

        type = null;
        try{
            ColumnType.getByTypeName(type);
        }catch (Exception e){
            assertTrue(e instanceof DataXException);
            assertTrue(e.getMessage().contains("Hbasereader 不支持该类型:null"));
        }
    }
}