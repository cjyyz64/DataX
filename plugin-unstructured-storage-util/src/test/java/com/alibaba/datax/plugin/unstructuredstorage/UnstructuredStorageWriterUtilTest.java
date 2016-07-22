package com.alibaba.datax.plugin.unstructuredstorage;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.plugin.unstructuredstorage.writer.TextCsvWriterManager;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;

public class UnstructuredStorageWriterUtilTest {

    @Test
    public void textWriterTest() throws IOException {
        StringWriter writer = new StringWriter();
        UnstructuredWriter unstructuredWriter = TextCsvWriterManager
                .produceUnstructuredWriter("text", ',', writer);
        unstructuredWriter.writeOneRecord(Arrays.asList("1", "2", "3", "abc"));
        System.out.println(writer.toString());
        Assert.assertTrue("1,2,3,abc\n".equals(writer.toString()));

        unstructuredWriter.writeOneRecord(Arrays.asList("1", "2", "3", ",abc"));
        System.out.println(writer.toString());
        Assert.assertTrue("1,2,3,abc\n1,2,3,,abc\n".equals(writer.toString()));

        unstructuredWriter.flush();
        unstructuredWriter.close();
        Assert.assertTrue("1,2,3,abc\n1,2,3,,abc\n".equals(writer.toString()));
    }

    @Test
    public void csvWriterTest() throws IOException {
        StringWriter writer = new StringWriter();
        UnstructuredWriter unstructuredWriter = TextCsvWriterManager
                .produceUnstructuredWriter("abc", ',', writer);
        Assert.assertTrue(unstructuredWriter.getClass().getName()
                .contains("CsvWriterImpl"));

        unstructuredWriter = TextCsvWriterManager.produceUnstructuredWriter(
                "csv", ',', writer);
        unstructuredWriter.writeOneRecord(Arrays.asList("1", "2", "3", "abc"));
        System.out.println(writer.toString());
        Assert.assertTrue("1,2,3,abc\n".equals(writer.toString()));

        unstructuredWriter.writeOneRecord(Arrays.asList("1", "2", "3", ",abc"));
        System.out.println(writer.toString());
        Assert.assertTrue("1,2,3,abc\n1,2,3,\",abc\"\n".equals(writer
                .toString()));

        unstructuredWriter.flush();
        unstructuredWriter.close();
        Assert.assertTrue("1,2,3,abc\n1,2,3,\",abc\"\n".equals(writer
                .toString()));

    }

}
