package com.alibaba.datax.plugin.writer.ftpwriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.datax.common.exception.ExceptionTracker;
import com.alibaba.datax.plugin.writer.ftpwriter.util.IFtpHelper;
import com.alibaba.datax.plugin.writer.ftpwriter.util.SftpHelperImpl;
import com.alibaba.datax.plugin.writer.ftpwriter.util.StandardFtpHelperImpl;
import com.google.common.collect.Sets;

public class FtpHelperTest {
    public static String host = "10.101.86.94";
    public static String sftp_username = "sftp";
    public static String ftp_username = "testftp";
    public static String password = "qweasd123";
    public static int sftp_port = 22;
    public static int ftp_port = 21;
    public static int timeout = 3600;

    /*
     * ftp工作在主动模式port使用tcp 21和20两个端口，而工作在被动pasv模式会工作在大于1024随机端口
     * 
     * 主动方式的FTP是这样的：客户端从一个任意的非特权端口N（N>1024）连接到FTP服务器的命令端口(即tcp
     * 21端口)。紧接着客户端开始监听端口N+1，并发送FTP命令“port
     * N+1”到FTP服务器。最后服务器会从它自己的数据端口（20）连接到客户端指定的数据端口
     * （N+1），这样客户端就可以和ftp服务器建立数据传输通道了。
     * 
     * 被动方式FTP中，命令连接和数据连接都由客户端。当开启一个FTP连接时，客户端打开两个任意的非特权本地端口（N >
     * 1024和N+1）。第一个端口连接服务器的21端口，但与主动方式的FTP不同，客户端不会提交PORT命令并允许服务器来回连它的数据端口，而是提交
     * PASV命令。这样做的结果是服务器会开启一个任意的非特权端口（P > 1024），并发送PORT
     * P命令给客户端。然后客户端发起从本地端口N+1到服务器的端口P的连接用来传送数据。
     */

    @Test
    public void sftp_ftpHelperTest() {
        // step1: login
        IFtpHelper ftpHelper = new SftpHelperImpl();
        ftpHelper.loginFtpServer(host, sftp_username, password, sftp_port,
                timeout);
        System.out.println("sftp login ok");
        Set<String> allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test", "unit_test.data");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("unit_test.data".equals(StringUtils.join(
                allFilesInDir.iterator(), ",")));
        // step2: logout
        ftpHelper.logoutFtpServer();
        System.out.println("sftp logout ok");
        try {
            allFilesInDir = ftpHelper.getAllFilesInDir(
                    "/home/haiwei.luo/datax_unit_test", "unit_test.data");
            Assert.assertTrue(false);
        } catch (Exception e) {
            // e.printStackTrace();
            String message = ExceptionTracker.trace(e);
            Assert.assertTrue(message
                    .contains("java.lang.NullPointerException"));
        }
        // step3: relogin
        ftpHelper.loginFtpServer(host, sftp_username, password, sftp_port,
                timeout);
        allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test", "unit_test.data");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("unit_test.data".equals(StringUtils.join(
                allFilesInDir.iterator(), ",")));

        // step4: mkdir
        ftpHelper.mkdir("/home/haiwei.luo/datax_unit_test");
        Assert.assertTrue(true);

        // step5: mkdir with no auth
        try {
            ftpHelper.mkdir("/root/datax_unit_test");
        } catch (Exception e) {
            // e.printStackTrace();
            String message = ExceptionTracker.trace(e);
            Assert.assertTrue(message.contains("Permission denied"));
        }

        // step6: mkdir in sub folder
        ftpHelper
                .mkdir("/home/haiwei.luo/datax_unit_test/sftp_datax_unit_new_create");
        Assert.assertTrue(true);

        // step7: get all file in dir whit prefix
        allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test/",
                "sftp_datax_unit_new_create");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("sftp_datax_unit_new_create".equals(StringUtils.join(
                allFilesInDir.iterator(), ",")));

        // step8: create a file @ remote file system
        OutputStream outputStream = ftpHelper
                .getOutputStream("/home/haiwei.luo/datax_unit_test/sftp_datax_unit_new_create/yixiao.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(outputStream));
        try {
            bufferedWriter.write("i am ok QAQ");
        } catch (IOException e) {
            e.printStackTrace();
        }
        IOUtils.closeQuietly(bufferedWriter);
        ftpHelper.completePendingCommand();

        // step9: get file created @ step8
        allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test/sftp_datax_unit_new_create/",
                "yixiao.txt");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("yixiao.txt".equals(StringUtils.join(
                allFilesInDir.iterator(), ",")));

        // step10: delete file created @ step8
        ftpHelper
                .deleteFiles(Sets
                        .newHashSet("/home/haiwei.luo/datax_unit_test/sftp_datax_unit_new_create/yixiao.txt"));
        Assert.assertTrue(true);

        allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test/sftp_datax_unit_new_create/",
                "yixiao");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("".equals(StringUtils.join(allFilesInDir.iterator(),
                ",")));

        /*
         * // step11: mkdir -p ftpHelper .mkdir(
         * "/home/haiwei.luo/datax_unit_test/sftp_datax_unit_new_create/dir1/dir2"
         * ); allFilesInDir = ftpHelper .getAllFilesInDir(
         * "/home/haiwei.luo/datax_unit_test/sftp_datax_unit_new_create/dir1/",
         * "dir"); System.out.println(StringUtils.join(allFilesInDir.iterator(),
         * ",")); Assert.assertTrue("dir2".equals(StringUtils.join(
         * allFilesInDir.iterator(), ",")));
         * 
         * // step12: rm -rf ftpHelper .deleteFiles(Sets .newHashSet(
         * "/home/haiwei.luo/datax_unit_test/sftp_datax_unit_new_create/"));
         * Assert.assertTrue(true);
         */

        System.out.println("i am ok QAQ");
        ftpHelper.logoutFtpServer();

    }

    @Test
    public void ftp_ftpHelperTest() {
        // step1: login
        IFtpHelper ftpHelper = new StandardFtpHelperImpl();
        ftpHelper.loginFtpServer(host, ftp_username, password, ftp_port,
                timeout);
        Set<String> allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test", "unit_test.data");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("unit_test.data".equals(StringUtils.join(
                allFilesInDir.iterator(), ",")));
        // step2: logout
        ftpHelper.logoutFtpServer();
        try {
            allFilesInDir = ftpHelper.getAllFilesInDir(
                    "/home/haiwei.luo/datax_unit_test", "unit_test.data");
            Assert.assertTrue(false);
        } catch (Exception e) {
            // e.printStackTrace();
            String message = ExceptionTracker.trace(e);
            Assert.assertTrue(message
                    .contains("java.lang.NullPointerException"));
        }
        // step3: relogin
        ftpHelper.loginFtpServer(host, ftp_username, password, ftp_port,
                timeout);
        allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test", "unit_test.data");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("unit_test.data".equals(StringUtils.join(
                allFilesInDir.iterator(), ",")));

        // step4: mkdir
        ftpHelper.mkdir("/home/haiwei.luo/datax_unit_test");
        Assert.assertTrue(true);

        // step5: mkdir with no auth
        try {
            ftpHelper.mkdir("/root/datax_unit_test");
        } catch (Exception e) {
            // e.printStackTrace();
            Assert.assertTrue(e
                    .getMessage()
                    .contains(
                            "Code:[FtpWriter-14], Description:[与ftp服务器连接异常.]. - 创建目录:/root/datax_unit_test时发生异常,请确认与ftp服务器的连接正常,拥有目录创建权限"));
        }

        // step6: mkdir in sub folder
        ftpHelper
                .mkdir("/home/haiwei.luo/datax_unit_test/ftp_datax_unit_new_create");
        Assert.assertTrue(true);

        // step7: get all file in dir whit prefix
        allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test/",
                "ftp_datax_unit_new_create");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("ftp_datax_unit_new_create".equals(StringUtils.join(
                allFilesInDir.iterator(), ",")));

        // step8: create a file @ remote file system
        OutputStream outputStream = ftpHelper
                .getOutputStream("/home/haiwei.luo/datax_unit_test/ftp_datax_unit_new_create/yixiao.txt");
        BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(outputStream));
        try {
            bufferedWriter.write("i am ok QAQ");
        } catch (IOException e) {
            e.printStackTrace();
        }
        IOUtils.closeQuietly(bufferedWriter);
        ftpHelper.completePendingCommand();

        // step9: get file created @ step8
        allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test/ftp_datax_unit_new_create/",
                "yixiao.txt");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("yixiao.txt".equals(StringUtils.join(
                allFilesInDir.iterator(), ",")));

        // step10: delete file created @ step8
        ftpHelper
                .deleteFiles(Sets
                        .newHashSet("/home/haiwei.luo/datax_unit_test/ftp_datax_unit_new_create/yixiao.txt"));
        Assert.assertTrue(true);

        allFilesInDir = ftpHelper.getAllFilesInDir(
                "/home/haiwei.luo/datax_unit_test/ftp_datax_unit_new_create/",
                "yixiao");
        System.out.println(StringUtils.join(allFilesInDir.iterator(), ","));
        Assert.assertTrue("".equals(StringUtils.join(allFilesInDir.iterator(),
                ",")));

        /*
         * // step11: mkdir -p ftpHelper .mkdir(
         * "/home/haiwei.luo/datax_unit_test/ftp_datax_unit_new_create/dir1/dir2"
         * ); allFilesInDir = ftpHelper .getAllFilesInDir(
         * "/home/haiwei.luo/datax_unit_test/ftp_datax_unit_new_create/dir1/",
         * "dir"); System.out.println(StringUtils.join(allFilesInDir.iterator(),
         * ",")); Assert.assertTrue("dir2".equals(StringUtils.join(
         * allFilesInDir.iterator(), ",")));
         * 
         * // step12: rm -rf ftpHelper .deleteFiles(Sets
         * .newHashSet("/home/haiwei.luo/datax_unit_test/ftp_datax_unit_new_create/"
         * )); Assert.assertTrue(true);
         */

        System.out.println("i am ok QAQ");
        ftpHelper.logoutFtpServer();

    }
}
