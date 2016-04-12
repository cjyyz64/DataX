package com.alibaba.datax.plugin.reader.streamreader;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.util.Configuration;

public class MinUpTest {

    @Test
    public void parseMixupFunctionsTest() throws NoSuchMethodException,
            SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException,
            NoSuchFieldException {
        StreamReader.Job job = new StreamReader.Job();
        Method parseMixupFunctions = StreamReader.Job.class.getDeclaredMethod(
                "parseMixupFunctions", Configuration.class);
        Field originalConfig = StreamReader.Job.class
                .getDeclaredField("originalConfig");
        originalConfig.setAccessible(true);
        originalConfig.set(job, Configuration.from("{}"));
        Field mixupFunctionPattern = StreamReader.Job.class
                .getDeclaredField("mixupFunctionPattern");
        mixupFunctionPattern.setAccessible(true);
        mixupFunctionPattern.set(job, Pattern.compile(
                Constant.MIXUP_FUNCTION_PATTERN, Pattern.CASE_INSENSITIVE));

        parseMixupFunctions.setAccessible(true);
        Configuration eachColumnConfig = Configuration
                .from("{'type': 'string'}");
        try {
            parseMixupFunctions.invoke(job, eachColumnConfig);
            Assert.assertTrue(false);
        } catch (Exception e) {
            System.out.println(e.getCause().getMessage());
            Assert.assertTrue(e.getCause().getMessage()
                    .contains("您提供配置文件有误，[value]是必填参数，不允许为空或者留白"));
        }

        eachColumnConfig = Configuration
                .from("{'type':'string','mixup':'random(10,20)','value':'string'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'string','value':'random(20,10)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'string','mixup':'random(10,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        try {
            eachColumnConfig = Configuration
                    .from("{'type':'string','mixup':'random(20,10)'}");
            parseMixupFunctions.invoke(job, eachColumnConfig);
            Assert.assertTrue(false);
        } catch (Exception e) {
            System.out.println(e.getCause().getMessage());
            Assert.assertTrue(e
                    .getCause()
                    .getMessage()
                    .contains(
                            "mixup混淆函数不合法[random(20,10)], 混淆函数random(param1, param2)的参数需要第一个小于等于第二个:(20, 10)"));
        }

        try {
            eachColumnConfig = Configuration
                    .from("{'type':'long','mixup':'RanDom(-10,10)'}");
            parseMixupFunctions.invoke(job, eachColumnConfig);
            Assert.assertTrue(false);
        } catch (Exception e) {
            System.out.println(e.getCause().getMessage());
            Assert.assertTrue(e
                    .getCause()
                    .getMessage()
                    .contains(
                            "mixup混淆函数不合法[RanDom(-10,10)], 混淆函数random(param1, param2)的参数不能为负数:(-10, 10)"));
        }

        eachColumnConfig = Configuration
                .from("{'type':'date','mixup':'random(2014-07-07 00:00:00,2016-07-07 00:00:00)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'date','mixup':'random(2014-07-07 00:00:00,2016-07-07 00:00:00)','dateFormat':\'yyyy-MM-ddHH:mm:ss\'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        try {
            eachColumnConfig = Configuration
                    .from("{'type':'date','mixup':'random(2016-07-07 00:00:00,2014-07-07 00:00:00)','dateFormat':\'yyyy-MM-ddHH:mm:ss\'}");
            parseMixupFunctions.invoke(job, eachColumnConfig);
            Assert.assertTrue(false);
        } catch (Exception e) {
            System.out.println(e.getCause().getMessage());
            Assert.assertTrue(e
                    .getCause()
                    .getMessage()
                    .contains(
                            "mixup混淆函数不合法[random(2016-07-07 00:00:00,2014-07-07 00:00:00)], 混淆函数random(param1, param2)的参数需要第一个小于等于第二个:(2016-07-07 00:00:00, 2014-07-07 00:00:00)"));
        }

        try {
            eachColumnConfig = Configuration
                    .from("{'type':'date','mixup':'random(2014-07-07 00:00:00,2016-07-0700:00:00)','dateFormat':'yy-yy-mM-ddHH:mm:ss'}");
            parseMixupFunctions.invoke(job, eachColumnConfig);
            Assert.assertTrue(false);
        } catch (Exception e) {
            System.out.println(e.getCause().getMessage());
            Assert.assertTrue(e
                    .getCause()
                    .getMessage()
                    .contains(
                            "dateFormat参数[yy-yy-mM-ddHH:mm:ss]和混淆函数random(param1, param2)的参数不匹配，解析错误:(2014-07-07 00:00:00, 2016-07-0700:00:00) - Unparseable date: \"2014-07-07 00:00:00\""));
        }

        eachColumnConfig = Configuration
                .from("{'type':'bytes','mixup':'RanDom(10,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'double','mixup':'RanDom(10,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bool','mixup':'RanDom(10,10)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bool','mixup':'RanDom(0,0)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bool','mixup':'RanDom(10,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        Assert.assertTrue(true);

    }

    @Test
    public void buildOneColumnTest() throws NoSuchMethodException,
            SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException,
            NoSuchFieldException, ParseException {
        StreamReader.Job job = new StreamReader.Job();
        Method parseMixupFunctions = StreamReader.Job.class.getDeclaredMethod(
                "parseMixupFunctions", Configuration.class);
        Field originalConfig = StreamReader.Job.class
                .getDeclaredField("originalConfig");
        originalConfig.setAccessible(true);
        originalConfig.set(job, Configuration.from("{}"));
        Field mixupFunctionPattern = StreamReader.Job.class
                .getDeclaredField("mixupFunctionPattern");
        mixupFunctionPattern.setAccessible(true);
        mixupFunctionPattern.set(job, Pattern.compile(
                Constant.MIXUP_FUNCTION_PATTERN, Pattern.CASE_INSENSITIVE));

        parseMixupFunctions.setAccessible(true);

        StreamReader.Task task = new StreamReader.Task();
        Method buildOneColumn = StreamReader.Task.class.getDeclaredMethod(
                "buildOneColumn", Configuration.class);
        Column column = null;

        buildOneColumn.setAccessible(true);
        Configuration eachColumnConfig = Configuration
                .from("{'type':'string','mixup':'random(10,20)','value':'string'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asString().equals("string"));

        eachColumnConfig = Configuration
                .from("{'type':'string','mixup':'random(10,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asString().length() >= 10
                && column.asString().length() <= 20);

        eachColumnConfig = Configuration
                .from("{'type':'date','mixup':'random(2014-07-07 00:00:00,2016-07-07 00:00:00)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long time1 = df.parse("2014-07-07 00:00:00").getTime();
        Long time2 = df.parse("2016-07-07 00:00:00").getTime();
        System.out.println(column.asString());
        Assert.assertTrue(column.asLong() >= time1 && column.asLong() <= time2);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'date','mixup':'random(2014-07-07 00:00:00,2016-07-07 00:00:00)','dateFormat':\'yyyy-MM-ddHH:mm:ss\'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asLong() >= time1 && column.asLong() <= time2);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bytes','mixup':'RanDom(10,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asString().length() >= 10
                && column.asString().length() <= 20);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'double','mixup':'RanDom(10,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asDouble() >= 10 && column.asDouble() <= 20);
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bool','mixup':'RanDom(10,10)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asString().equals("false")
                || column.asString().equals("true"));
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bool','mixup':'RanDom(0,0)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asString().equals("false")
                || column.asString().equals("true"));
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bool','mixup':'RanDom(10,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asString().equals("false")
                || column.asString().equals("true"));
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bool','mixup':'RanDom(0,20)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asString().equals("true"));
        Assert.assertTrue(true);

        eachColumnConfig = Configuration
                .from("{'type':'bool','mixup':'RanDom(20,0)'}");
        parseMixupFunctions.invoke(job, eachColumnConfig);
        column = (Column) buildOneColumn.invoke(task, eachColumnConfig);
        System.out.println(column.asString());
        Assert.assertTrue(column.asString().equals("false"));
        Assert.assertTrue(true);
    }

    @Test
    public void testRandom() throws InterruptedException {
        MyRandom ran1 = new MyRandom();
        MyRandom ran2 = new MyRandom();
        // ran1.start();
        // ran2.start();
        // Thread.currentThread().join();
    }

    class MyRandom extends Thread {
        @Override
        public void run() {
            while (true) {
                System.out.println(String.format(
                        "Thread:%s randomString:%s randomLong: %s", Thread
                                .currentThread().getId(), RandomStringUtils
                                .randomAlphanumeric(20), RandomUtils.nextLong(
                                0, 100)));
            }

        }
    }
}
