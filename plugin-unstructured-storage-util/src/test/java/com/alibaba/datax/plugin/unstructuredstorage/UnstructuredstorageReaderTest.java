package com.alibaba.datax.plugin.unstructuredstorage;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class UnstructuredstorageReaderTest {

    @Test
    public void testSplit() {
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

        // UnstructuredStorageReaderUtil.splitOneLine(inputLine, delimiter)
    }
}
