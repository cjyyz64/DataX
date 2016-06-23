package com.alibaba.datax.plugin.writer.ftpwriter;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.ftpwriter.FtpWriter.Job;
import com.alibaba.datax.plugin.writer.ftpwriter.FtpWriter.Task;
import com.alibaba.datax.plugin.writer.ftpwriter.util.IFtpHelper;
import com.alibaba.fastjson.util.IOUtils;
import com.google.common.collect.Sets;

public class FtpWriterTest {

    @Test
    public void testInit() {
        FtpWriter.Job job = new Job();
        String config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp'}";
        try {
            job.setPluginJobConf(Configuration.from(config));
            job.init();
        } catch (Exception e) {
            Assert.assertTrue(e
                    .getMessage()
                    .contains(
                            "Description:[您缺失了必须填写的参数值.]. - 您提供配置文件有误，[host]是必填参数，不允许为空或者留白"));
        }

        config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','host':'10.101.86.94','port':'22','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp'}";
        try {
            job.setPluginJobConf(Configuration.from(config));
            job.init();
        } catch (Exception e) {
            Assert.assertTrue(e
                    .getMessage()
                    .contains(
                            "Description:[您缺失了必须填写的参数值.]. - 您提供配置文件有误，[username]是必填参数，不允许为空或者留白"));
        }

        config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','host':'10.101.86.94','port':'22','username':'sftp','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp'}";
        try {
            job.setPluginJobConf(Configuration.from(config));
            job.init();
        } catch (Exception e) {
            Assert.assertTrue(e
                    .getMessage()
                    .contains(
                            "Description:[您缺失了必须填写的参数值.]. - 您提供配置文件有误，[password]是必填参数，不允许为空或者留白"));
        }
        config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'yixiao'}";
        try {
            job.setPluginJobConf(Configuration.from(config));
            job.init();
        } catch (Exception e) {
            // e.printStackTrace();
            Assert.assertTrue(e
                    .getMessage()
                    .contains(
                            "Description:[您填写的参数值不合法.]. - 仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [yixiao]"));
        }
        config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'yixiao','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp'}";
        try {
            job.setPluginJobConf(Configuration.from(config));
            job.init();
        } catch (Exception e) {
            // e.printStackTrace();
            Assert.assertTrue(e
                    .getMessage()
                    .contains(
                            "Description:[您填写的参数值不合法.]. - 仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [yixiao]"));
        }
        config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp','fieldDelimiter':'yixiao'}";
        try {
            job.setPluginJobConf(Configuration.from(config));
            job.init();
        } catch (Exception e) {
            // e.printStackTrace();
            Assert.assertTrue(e
                    .getMessage()
                    .contains(
                            "Description:[您填写的参数值不合法.]. - 仅仅支持单字符切分, 您配置的切分为 : [yixiao]"));
        }

        // port
        config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','host':'10.101.86.94','port_':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        Assert.assertTrue(job.getPluginJobConf().getInt(Key.PORT) == 22);

        config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','host':'10.101.86.94','port_':'22','username':'testftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'ftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        Assert.assertTrue(job.getPluginJobConf().getInt(Key.PORT) == 21);

        config = "{'path':'/home/yuxiong.wangyx/datax-test/writers/ftpwriter/bvt_data','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        Assert.assertTrue(true);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPrepare() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        FtpWriter.Job job = new Job();
        IFtpHelper ftpHelper = null;
        Set<String> allFileExists = null;
        Field ftpHelperField = null;
        Field allFileExistsField = null;
        String config = null;
        OutputStream outputStream = null;

        // ------------------- ftp prepare ---------------------

        // append
        config = "{'path':'/home/haiwei.luo/datax_unit_test/ftp_writer_test','host':'10.101.86.94','port':'21','username':'testftp','password':'qweasd123','fileName':'yixiao','writeMode':'append','encoding':'utf8','header':[1,2,3,4,5],'protocol':'ftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        ftpHelperField = job.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(job);
        allFileExistsField = job.getClass().getDeclaredField("allFileExists");
        allFileExistsField.setAccessible(true);
        outputStream = ftpHelper.getOutputStream(job.getPluginJobConf()
                .getString(Key.PATH) + "/yixiao_123");
        IOUtils.close(outputStream);
        ftpHelper.completePendingCommand();
        job.prepare();
        allFileExists = (Set<String>) allFileExistsField.get(job);
        Assert.assertTrue(allFileExists.contains("yixiao_123"));

        // nonConflict
        config = "{'path':'/home/haiwei.luo/datax_unit_test/ftp_writer_test','host':'10.101.86.94','port':'21','username':'testftp','password':'qweasd123','fileName':'yixiao','writeMode':'nonConflict','encoding':'utf8','header':[1,2,3,4,5],'protocol':'ftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        ftpHelperField = job.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(job);
        allFileExistsField = job.getClass().getDeclaredField("allFileExists");
        allFileExistsField.setAccessible(true);
        try {
            job.prepare();
            allFileExists = (Set<String>) allFileExistsField.get(job);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // truncate
        config = "{'path':'/home/haiwei.luo/datax_unit_test/ftp_writer_test','host':'10.101.86.94','port':'21','username':'testftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'ftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        ftpHelperField = job.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(job);
        allFileExistsField = job.getClass().getDeclaredField("allFileExists");
        allFileExistsField.setAccessible(true);
        try {
            job.prepare();
            allFileExists = (Set<String>) allFileExistsField.get(job);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // nonConflict
        config = "{'path':'/home/haiwei.luo/datax_unit_test/ftp_writer_test','host':'10.101.86.94','port':'21','username':'testftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'ftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        ftpHelperField = job.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(job);
        allFileExistsField = job.getClass().getDeclaredField("allFileExists");
        allFileExistsField.setAccessible(true);
        job.prepare();
        allFileExists = (Set<String>) allFileExistsField.get(job);
        outputStream = ftpHelper.getOutputStream(job.getPluginJobConf()
                .getString(Key.PATH) + "/yixiao_123");
        IOUtils.close(outputStream);
        job.destroy();

        // ------------------- sftp prepare ---------------------
        // append
        config = "{'path':'/home/haiwei.luo/datax_unit_test/sftp_writer_test','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'append','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        ftpHelperField = job.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(job);
        allFileExistsField = job.getClass().getDeclaredField("allFileExists");
        allFileExistsField.setAccessible(true);
        outputStream = ftpHelper.getOutputStream(job.getPluginJobConf()
                .getString(Key.PATH) + "/yixiao_123");
        IOUtils.close(outputStream);
        ftpHelper.completePendingCommand();
        job.prepare();
        allFileExists = (Set<String>) allFileExistsField.get(job);
        Assert.assertTrue(allFileExists.contains("yixiao_123"));

        // nonConflict
        config = "{'path':'/home/haiwei.luo/datax_unit_test/sftp_writer_test','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'nonConflict','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        ftpHelperField = job.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(job);
        allFileExistsField = job.getClass().getDeclaredField("allFileExists");
        allFileExistsField.setAccessible(true);
        try {
            job.prepare();
            allFileExists = (Set<String>) allFileExistsField.get(job);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // truncate
        config = "{'path':'/home/haiwei.luo/datax_unit_test/sftp_writer_test','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        ftpHelperField = job.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(job);
        allFileExistsField = job.getClass().getDeclaredField("allFileExists");
        allFileExistsField.setAccessible(true);
        try {
            job.prepare();
            allFileExists = (Set<String>) allFileExistsField.get(job);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // nonConflict
        config = "{'path':'/home/haiwei.luo/datax_unit_test/sftp_writer_test','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'yixiao','writeMode':'truncate','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp','fieldDelimiter':'Y'}";
        job.setPluginJobConf(Configuration.from(config));
        job.init();
        ftpHelperField = job.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(job);
        allFileExistsField = job.getClass().getDeclaredField("allFileExists");
        allFileExistsField.setAccessible(true);
        job.prepare();
        allFileExists = (Set<String>) allFileExistsField.get(job);
        outputStream = ftpHelper.getOutputStream(job.getPluginJobConf()
                .getString(Key.PATH) + "/yixiao_123");
        IOUtils.close(outputStream);
        job.destroy();
    }

    @Test
    public void testStartWrite() throws NoSuchFieldException,
            SecurityException, IllegalArgumentException, IllegalAccessException {
        FtpWriter.Task task = new Task();
        String config = null;
        String content = null;
        String testFilePath = null;
        IFtpHelper ftpHelper = null;
        Field ftpHelperField = null;

        // ----------------- ftp start write -----------------
        config = "{'path':'/home/haiwei.luo/datax_unit_test/ftp_writer_test','host':'10.101.86.94','port':'21','username':'testftp','password':'qweasd123','fileName':'task_write_yixiao','writeMode':'append','encoding':'utf8','header':[1,2,3,4,5],'protocol':'ftp','fieldDelimiter':','}";
        task.setPluginJobConf(Configuration.from(config));
        testFilePath = task.getPluginJobConf().getString(Key.PATH)
                + "/task_write_yixiao";
        task.init();
        ftpHelperField = task.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(task);
        ftpHelper.deleteFiles(Sets.newHashSet(testFilePath));
        task.startWrite(this.getRecordReceiver());
        content = ftpHelper.getRemoteFileContent(testFilePath);
        System.out.println(content);
        Assert.assertTrue("1,2,3,4,5\nyixiao_0,yixiao_1".equals(content));
        task.destroy();

        // ----------------- sftp start write -----------------
        task = new Task();
        config = "{'path':'/home/haiwei.luo/datax_unit_test/sftp_writer_test','host':'10.101.86.94','port':'22','username':'sftp','password':'qweasd123','fileName':'task_write_yixiao','writeMode':'append','encoding':'utf8','header':[1,2,3,4,5],'protocol':'sftp','fieldDelimiter':','}";
        task.setPluginJobConf(Configuration.from(config));
        testFilePath = task.getPluginJobConf().getString(Key.PATH)
                + "/task_write_yixiao";
        task.init();
        ftpHelperField = task.getClass().getDeclaredField("ftpHelper");
        ftpHelperField.setAccessible(true);
        ftpHelper = (IFtpHelper) ftpHelperField.get(task);
        ftpHelper.deleteFiles(Sets.newHashSet(testFilePath));
        task.startWrite(this.getRecordReceiver());
        content = ftpHelper.getRemoteFileContent(testFilePath);
        System.out.println(content);
        Assert.assertTrue("1,2,3,4,5\nyixiao_0,yixiao_1".equals(content));
        task.destroy();
    }

    private RecordReceiver getRecordReceiver() {
        RecordReceiver recoreReceiver = new RecordReceiver() {
            public boolean gotARecord = false;

            @Override
            public void shutdown() {
            }

            @Override
            public Record getFromReader() {
                if (!this.gotARecord) {
                    this.gotARecord = true;
                    return new Record() {
                        @Override
                        public void setColumn(int i, Column column) {
                        }

                        @Override
                        public int getMemorySize() {
                            return 2;
                        }

                        @Override
                        public int getColumnNumber() {
                            return 2;
                        }

                        @Override
                        public Column getColumn(int i) {
                            return new StringColumn("yixiao_" + i);
                        }

                        @Override
                        public int getByteSize() {
                            return 0;
                        }

                        @Override
                        public void addColumn(Column column) {

                        }
                    };
                } else {
                    return null;
                }
            }
        };
        return recoreReceiver;
    }
}
