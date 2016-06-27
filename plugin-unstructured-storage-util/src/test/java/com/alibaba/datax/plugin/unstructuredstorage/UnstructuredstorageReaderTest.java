package com.alibaba.datax.plugin.unstructuredstorage;

import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.csvreader.CsvReader;

public class UnstructuredstorageReaderTest {

    @Test
    public void testSplitStringUtils() {
        // 这个是出现一部分split一部分
        // StringUtils.split(String, String)
        String[] splitedStrs = UnstructuredStorageReaderUtil.splitOneLine(
                "xyzabc123a456b789cxyzabxyzbcxyz", "abc");
        String result = JSON.toJSONString(splitedStrs,
                SerializerFeature.UseSingleQuotes);
        System.out.println(result);
        Assert.assertTrue("['xyz','123','456','789','xyz','xyz','xyz']"
                .equals(result));

        // warn: 空字符串被丢弃
        splitedStrs = UnstructuredStorageReaderUtil.splitOneLine("a,,b,c", ",");
        result = JSON.toJSONString(splitedStrs,
                SerializerFeature.UseSingleQuotes);
        System.out.println(result);
        Assert.assertTrue("['a','b','c']".equals(result));
    }

    public String[] wrapperSplitBufferedReader(String line, char fieldDelimiter)
            throws IOException {
        StringReader sr = new StringReader(line);
        CsvReader csvReader = new CsvReader(sr);
        csvReader.setDelimiter(fieldDelimiter);
        return UnstructuredStorageReaderUtil.splitBufferedReader(csvReader);
    }

    @Test
    public void testSplitCsvReader() throws IOException {
        char fieldDelimiter = ',';
        String line = "pq1,2,2,1,-,4,99,-,4,99,1,99,0,Z,,6,0,4,70,0,1,/3257651678047895553/2016/05/23/14/pq1/14084865301.jpg?fid=1138273,,,,156,1,0,0,,pq1,2,2,2016-05-23 14:08:48.316,0,,,,2016-05-23 14:06:31,0568,1092,1382,2042,0915,1567,1035,1607,,,,,,,0,0,0,0,98,,,,2,2,2,2,2,,0,tmsserver,abcdefgh,abcdefgh";
        String[] result = this.wrapperSplitBufferedReader(line, fieldDelimiter);
        String message = StringUtils.join(result, ",");
        System.out.println(message);
        Assert.assertTrue(result.length == 71);
        Assert.assertTrue("pq1,2,2,1,-,4,99,-,4,99,1,99,0,Z,,6,0,4,70,0,1,/3257651678047895553/2016/05/23/14/pq1/14084865301.jpg?fid=1138273,,,,156,1,0,0,,pq1,2,2,2016-05-23 14:08:48.316,0,,,,2016-05-23 14:06:31,0568,1092,1382,2042,0915,1567,1035,1607,,,,,,,0,0,0,0,98,,,,2,2,2,2,2,,0,tmsserver,abcdefgh,abcdefgh"
                .equalsIgnoreCase(message));

        fieldDelimiter = '\t';
        line = "a\tb\tc";
        result = this.wrapperSplitBufferedReader(line, fieldDelimiter);
        message = StringUtils.join(result, ",");
        System.out.println(message);
        Assert.assertTrue(result.length == 3);
        Assert.assertTrue("a,b,c".equals(message));

        fieldDelimiter = ',';
        line = "1,true,22.22,\"LuoHW-\"\"'\"\"\",2014-02-22";
        result = this.wrapperSplitBufferedReader(line, fieldDelimiter);
        message = StringUtils.join(result, ",");
        System.out.println(message);
        Assert.assertTrue(result.length == 5);
        Assert.assertTrue("1,true,22.22,LuoHW-\"'\",2014-02-22".equals(message));

        result = UnstructuredStorageReaderUtil.splitOneLine(line, ',');
        message = StringUtils.join(result, ",");
        System.out.println(message);
        Assert.assertTrue(result.length == 5);
        Assert.assertTrue("1,true,22.22,LuoHW-\"'\",2014-02-22".equals(message));

        fieldDelimiter = ',';
        line = "1,true,22.22,LuoHW-\"'\",2014-02-22";
        result = this.wrapperSplitBufferedReader(line, fieldDelimiter);
        message = StringUtils.join(result, ",");
        System.out.println(message);
        Assert.assertTrue(result.length == 5);
        Assert.assertTrue("1,true,22.22,LuoHW-\"'\",2014-02-22".equals(message));
    }
}
