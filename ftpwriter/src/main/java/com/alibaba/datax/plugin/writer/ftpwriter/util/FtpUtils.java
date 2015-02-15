package com.alibaba.datax.plugin.writer.ftpwriter.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.ftpwriter.Constant;
import com.alibaba.datax.plugin.writer.ftpwriter.FtpWriterErrorCode;

//TODO finally close ftpClient
public class FtpUtils {
	private static final Logger LOG = LoggerFactory.getLogger(FtpUtils.class);

	public static FTPClient getFTPClient(String ip, int port, String username,
			String password) {
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.connect(ip, port);
		} catch (Exception e) {
			throw DataXException
					.asDataXException(FtpWriterErrorCode.CONNECT_FTP_ERROR,
							String.format("连接ftp网络异常, 您配置的ip [%s] port [%s]",
									ip, port));
		}

		try {
			boolean flag = ftpClient.login(username, password);
			if (flag) {
				ftpClient.setConnectTimeout(Constant.KEEP_CONNECTION_TIMEOUT);
				return ftpClient;
			} else {
				throw DataXException.asDataXException(
						FtpWriterErrorCode.CONNECT_FTP_ERROR,
						"连接ftp网络异常, 请关注您配置的用户名或者密码");
			}
		} catch (IOException e) {
			throw DataXException.asDataXException(
					FtpWriterErrorCode.CONNECT_FTP_ERROR,
					"连接ftp网络异常, 请关注您配置的用户名或者密码");
		}
	}

	public static void closeConnection(FTPClient ftpClient) {
		try {
			ftpClient.disconnect();
		} catch (IOException e) {
			throw DataXException.asDataXException(
					FtpWriterErrorCode.CONNECT_FTP_ERROR, "关闭ftp连接异常");
		}
	}

	public static Set<String> listFiles(FTPClient ftpClient, String path) {
		Set<String> allRemoteFiles = new HashSet<String>();
		try {
			FTPFile[] files = ftpClient.listFiles(path);
			for (FTPFile file : files) {
				allRemoteFiles.add(file.getName());
			}
		} catch (IOException e) {
			throw DataXException.asDataXException(
					FtpWriterErrorCode.LIST_FILES_ERROR,
					String.format("查看远程目录[%s]异常, 请注意目录路径和权限", path));
		}
		return allRemoteFiles;
	}

	public static boolean makeSureDirectory(FTPClient ftpClient, String path) {
		boolean flag = false;
		try {
			flag = ftpClient.makeDirectory(path);
		} catch (IOException e) {
			throw DataXException.asDataXException(
					FtpWriterErrorCode.LIST_FILES_ERROR,
					String.format("查看远程目录[%s]异常, 请注意目录路径和权限", path));
		}
		return flag;
	}

	public static void deleteDir(FTPClient ftpClient, String ftpPath) {
		try {
			iterateDelete(ftpClient, ftpPath);
		} catch (IOException e) {
			throw DataXException.asDataXException(
					FtpWriterErrorCode.CONNECT_FTP_ERROR,
					String.format("清除目录 [%s] 异常", ftpPath));
		}
	}

	public static boolean iterateDelete(FTPClient ftpClient, String ftpPath)
			throws IOException {
		FTPFile[] files = ftpClient.listFiles(ftpPath);
		boolean flag = false;
		for (FTPFile f : files) {
			String path = ftpPath + IOUtils.DIR_SEPARATOR + f.getName();
			if (f.isFile()) {
				// 是文件就删除文件
				ftpClient.deleteFile(path);
			} else if (f.isDirectory()) {
				iterateDelete(ftpClient, path);
			}
		}
		// 每次删除文件夹以后就去查看该文件夹下面是否还有文件，没有就删除该空文件夹
		FTPFile[] files2 = ftpClient.listFiles(ftpPath);
		if (files2.length == 0) {
			flag = ftpClient.removeDirectory(ftpPath);
		} else {
			flag = false;
		}
		return flag;
	}
}
