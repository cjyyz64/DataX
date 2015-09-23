package com.alibaba.datax.plugin.writer.tairwriter;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.channel.memory.MemoryChannel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.container.CoreConstant;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

/**
 * Created by liqiang on 15/9/23.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TariWriterTest {
    Logger log = LoggerFactory.getLogger(TariWriterTest.class);

    Configuration conf;
    RecordSender recordSender;
    RecordReceiver recordReceiver;
    TaskPluginCollector pluginCollector;

    @Before
    public void setUp() throws Exception {
        String config = Thread.currentThread().getContextClassLoader().getResource("tair_config.json").getFile();
        conf = Configuration.from(new File(config));

        String all_job_config = Thread.currentThread().getContextClassLoader().getResource("all.json").getFile();
        Configuration configuration = Configuration.from(new File(all_job_config));
        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 1);


        Channel channel3 = new MemoryChannel(configuration);
        channel3.setCommunication(new Communication());

        pluginCollector = mock(TaskPluginCollector.class);

        recordSender = new BufferedRecordExchanger(
                channel3, pluginCollector);

        recordReceiver = spy(new BufferedRecordExchanger(
                channel3, pluginCollector));
    }



    @Test
    public void test001ThreadException() {
        AtomicReference<Exception> threadException = new AtomicReference<Exception>();
        System.out.println(threadException.get());

        Assert.assertTrue(threadException.get() == null);



        Exception e = new RuntimeException("test");
        threadException.set(e);
        System.out.println(threadException.get());
        Assert.assertTrue(threadException.get() != null);
        log.info(threadException.get().toString());
        Assert.assertEquals(threadException.get(), e);
    }


    @Test
    public void test002Normal() throws Exception {


        log.info(" => " + conf.get(Key.TIMEOUT));
        TairWriter.Task t=new TairWriter.Task();
        t.setPluginJobConf(conf);
        t.setTaskPluginCollector(pluginCollector);

        t.init();

        Field work = t.getClass().getDeclaredField("works");
        work.setAccessible(true);
        TairWriterMultiWorker[] tmworks= (TairWriterMultiWorker[])work.get(t);

        //所有线程work均启动
        Assert.assertEquals(40, tmworks.length);
        for(TairWriterMultiWorker twwork:tmworks){
            Assert.assertTrue(twwork.isAlive());
        }


        //启动写线程
        //通过验证脏数据 来验证写，因此只创建一个column的record

        Thread write = new Thread(new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < 11112; i++) {
                    Record record = RecordProducer.produceRecord1();
                    record.setColumn(0, new LongColumn(i));
                    recordSender.sendToWriter(record);
                }
                //结束写线程
                recordSender.sendToWriter(TerminateRecord.get());
                recordSender.flush();
            }
        });

        write.start();

        //启动写
        t.startWrite(recordReceiver);

        //验证数据一条不差的写出。 通过验证脏数据
        verify(recordReceiver,times(11113)).getFromReader();
        verify(pluginCollector,times(11112)).collectDirtyRecord(any(Record.class),anyString());

    }

    @Test
    //测试条数小于channel buffer
    public void test003Normal2() throws Exception {


        log.info(" => " + conf.get(Key.TIMEOUT));
        TairWriter.Task t=new TairWriter.Task();
        t.setPluginJobConf(conf);
        t.setTaskPluginCollector(pluginCollector);

        t.init();

        Field work = t.getClass().getDeclaredField("works");
        work.setAccessible(true);
        TairWriterMultiWorker[] tmworks= (TairWriterMultiWorker[])work.get(t);

        //所有线程work均启动
        Assert.assertEquals(40, tmworks.length);
        for(TairWriterMultiWorker twwork:tmworks){
            Assert.assertTrue(twwork.isAlive());
        }


        //启动写线程
        //通过验证脏数据 来验证写，因此只创建一个column的record

        Thread write = new Thread(new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < 21; i++) {
                    Record record = RecordProducer.produceRecord1();
                    record.setColumn(0, new LongColumn(i));
                    recordSender.sendToWriter(record);
                }
                //结束写线程
                recordSender.sendToWriter(TerminateRecord.get());
                recordSender.flush();
            }
        });

        write.start();

        //启动写
        t.startWrite(recordReceiver);

        //验证数据一条不差的写出。 通过验证脏数据
        verify(recordReceiver,times(22)).getFromReader();
        verify(pluginCollector,times(21)).collectDirtyRecord(any(Record.class),anyString());

    }


    @Test(expected = DataXException.class)
    public void testNormal004WorkException() throws Exception {

        //构造脏数据收集抛出异常
        doThrow(new RuntimeException("mock Exception")).when(pluginCollector).collectDirtyRecord(any(Record.class),anyString());

        log.info(" => " + conf.get(Key.TIMEOUT));
        TairWriter.Task t=new TairWriter.Task();
        t.setPluginJobConf(conf);
        t.setTaskPluginCollector(pluginCollector);

        t.init();

        Field work = t.getClass().getDeclaredField("works");
        work.setAccessible(true);
        TairWriterMultiWorker[] tmworks= (TairWriterMultiWorker[])work.get(t);

        //所有线程work均启动
        Assert.assertEquals(40, tmworks.length);
        for(TairWriterMultiWorker twwork:tmworks){
            Assert.assertTrue(twwork.isAlive());
        }


        //启动写线程
        //通过验证脏数据 来验证写，因此只创建一个column的record

        Thread write = new Thread(new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < 21; i++) {
                    Record record = RecordProducer.produceRecord1();
                    record.setColumn(0, new LongColumn(i));
                    recordSender.sendToWriter(record);
                }
                //结束写线程
                recordSender.sendToWriter(TerminateRecord.get());
                recordSender.flush();
            }
        });

        write.start();

        //启动写
        t.startWrite(recordReceiver);


    }


    public static Record produceRecord() {

        try {
            Record record = new DefaultRecord();
            record.addColumn(ColumnProducer.produceLongColumn(1));
            record.addColumn(ColumnProducer.produceStringColumn("bazhen"));
            record.addColumn(ColumnProducer.produceBoolColumn(true));
            record.addColumn(ColumnProducer.produceDateColumn(System
                    .currentTimeMillis()));
            record.addColumn(ColumnProducer.produceBytesColumn("bazhen"
                    .getBytes("utf-8")));
            return record;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}