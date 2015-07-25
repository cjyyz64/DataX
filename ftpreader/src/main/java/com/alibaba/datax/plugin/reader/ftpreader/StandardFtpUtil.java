package com.alibaba.datax.plugin.reader.ftpreader;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.ftpreader.FtpReader.Job;

public class StandardFtpUtil {
	private static final Logger LOG = LoggerFactory.getLogger(Job.class);
	/**
	 * 
	* @Title: connectServer 
	* @Description: 通过标准ftp协议连接ftp服务器的工具类
	* @param @param host
	* @param @param username
	* @param @param password
	* @param @param port
	* @param @param timeout
	* @param @param connectMode
	* @param @return     
	* @return FTPClient 返回连接了ftp服务器的手柄
	* @throws
	 */
	public FTPClient connectServer(String host, String username, String password, int port, int timeout,
			String connectMode) {
		FTPClient ftpClient = new FTPClient();
		try {
			// 连接
			ftpClient.connect(host, port);
			// 登录
			ftpClient.login(username, password);
			ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
			ftpClient.setConnectTimeout(timeout);
			if ("PASV".equals(connectMode)) {
				ftpClient.enterRemotePassiveMode();
				ftpClient.enterLocalPassiveMode();
			} else if ("PORT".equals(connectMode)) {
				ftpClient.enterLocalActiveMode();
				// ftpClient.enterRemoteActiveMode(host, port);
			}
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
						"message:host =" + host + ",username = " + username + ",port =" + port);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message);
			}
		} catch (UnknownHostException e) {
			String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
		} catch (IllegalArgumentException e) {
			String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
		} catch (Exception e) {
			String message = String.format("与ftp服务器建立连接失败 : [%s]",
					"message:host =" + host + ",username = " + username + ",port =" + port);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
		}
		return ftpClient;
	}

	/**
	 * 
	* @Title: closeServer 
	* @Description: 与ftp服务器断开连接
	* @param @param ftpClient   连接了ftp服务器的手柄
	* @return void 
	* @throws
	 */
	public void closeServer(FTPClient ftpClient) {
		if (ftpClient.isConnected()) {
			try {
				ftpClient.logout();
				ftpClient.disconnect();
			} catch (IOException e) {
				String message = String.format("与ftp服务器断开连接失败");
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_DISCONNECT, message, e);
			}
		}
	}

	/**
	 * 
	* @Title: isDirExist 
	* @Description: 判断指定路径的目录是否存在，根据能否改变为工作目录判断
	* @param @param ftpClient 连接了ftp服务器的手柄
	* @param @param directory
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public boolean isDirExist(FTPClient ftpClient, String directory) {
		try {
			return ftpClient.changeWorkingDirectory(directory);
		} catch (IOException e) {
			String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directory);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
	}

	/**
	 * 
	* @Title: isFileExist 
	* @Description: 判断指定路径的文件是否存在
	* @param @param ftpClient 连接了ftp服务器的手柄
	* @param @param srcSftpFilePath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public boolean isFileExist(FTPClient ftpClient, String srcSftpFilePath) {
		boolean isExitFlag = false;
		try {
			FTPFile[] ftpFiles = ftpClient.listFiles(srcSftpFilePath);
			if (ftpFiles.length == 1 && ftpFiles[0].isFile()) {
				isExitFlag = true;
			}
		} catch (IOException e) {
			String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", srcSftpFilePath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
		return isExitFlag;
	}
	/**
	 * 
	* @Title: isLink 
	* @Description: 判读指定路径的文件是否是链接
	* @param @param ftpClient 连接了ftp服务器的手柄
	* @param @param srcSftpFilePath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public boolean isLink(FTPClient ftpClient, String srcSftpFilePath) {
		boolean isExitFlag = false;
		try {
			FTPFile[] ftpFiles = ftpClient.listFiles(srcSftpFilePath);
			if (ftpFiles.length == 1 && ftpFiles[0].isSymbolicLink()) {
				isExitFlag = true;
			}
		} catch (IOException e) {
			String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", srcSftpFilePath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
		return isExitFlag;
	}

	/**
	 * 
	* @Title: getListFiles 
	* @Description: 获取指定路径下符合条件的所有文件的绝对路径   
	* @param @param ftpClient 连接了ftp服务器的手柄
	* @param @param eachPath
	* @param @param parentLevel 父目录的递归层数（首次为0）
	* @param @param maxTraversalLevel 允许的最大递归层数
	* @param @return     
	* @return HashSet<String> 
	* @throws
	 */
	HashSet<String> sourceFiles = new HashSet<String>();
	public HashSet<String> getListFiles(FTPClient ftpClient, String eachPath, int parentLevel, int maxTraversalLevel) {
		if(parentLevel < maxTraversalLevel){
			String parentPath = null;// 父级目录,以'/'结尾
			int pathLen = eachPath.length();
			if (eachPath.contains("*") || eachPath.contains("?")) {
				// path是正则表达式
				int endMark;
				for (endMark = 0; endMark < pathLen; endMark++) {
					if ('*' != eachPath.charAt(endMark) && '?' != eachPath.charAt(endMark)) {
						continue;
					} else {
						break;
					}
				}
				int lastDirSeparator = eachPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR);
				String subPath  = eachPath.substring(0,lastDirSeparator + 1);
				if (isDirExist(ftpClient, subPath)) {
					parentPath = subPath;
				} else {
					String message = String.format("不能进入目录：[%s]," + "请确认您的配置项path:[%s]存在，且配置的用户有权限进入", subPath,
							eachPath);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
				}
			} else if (isDirExist(ftpClient, eachPath)) {
				// path是目录
				if (eachPath.charAt(pathLen - 1) == IOUtils.DIR_SEPARATOR) {
					parentPath = eachPath;
				} else {
					parentPath = eachPath + IOUtils.DIR_SEPARATOR;
				}
			} else if (isFileExist(ftpClient, eachPath)) {
				// path指向具体文件
				sourceFiles.add(eachPath);
				return sourceFiles;
			} else if(isLink(ftpClient, eachPath)){
				//path是链接文件
				String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", eachPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
			}else {
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", eachPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
			}

			try {
				FTPFile[] fs = ftpClient.listFiles(eachPath);
				for (FTPFile ff : fs) {
					String strName = ff.getName();
					String filePath = parentPath + strName;
					if (ff.isDirectory()) {
						if (!(strName.equals(".") || strName.equals(".."))) {
							//递归处理
							getListFiles(ftpClient, filePath, parentLevel+1, maxTraversalLevel);
						}
					} else if (ff.isFile()) {
						// 是文件
						sourceFiles.add(filePath);						
					} else if(ff.isSymbolicLink()){
						//是链接文件
						String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", filePath);
						LOG.error(message);
						throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
					}else {
						String message = String.format("请确认path:[%s]存在，且配置的用户有权限读取", filePath);
						LOG.error(message);
						throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
					}
				} // end for FTPFile
			} catch (IOException e) {
				String message = String.format("获取path：[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常", eachPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
			}
			return sourceFiles;
			
		} else{
			//超出最大递归层数
			String message = String.format("获取path：[%s] 下文件列表时超出最大层数,请确认路径[%s]下不存在软连接文件", eachPath, eachPath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.OUT_MAX_DIRECTORY_LEVEL, message);
		}
	}
	/**
	 * 
	* @Title: getAllFiles 
	* @Description: 获取指定路径列表下符合条件的所有文件的绝对路径 
	* @param @param ftpClient
	* @param @param srcPaths
	* @param @param parentLevel 父目录的递归层数（首次为0）
	* @param @param maxTraversalLevel 允许的最大递归层数
	* @param @return     
	* @return HashSet<String> 
	* @throws
	 */
	public HashSet<String> getAllFiles(FTPClient ftpClient, List<String> srcPaths, int parentLevel, int maxTraversalLevel){
		HashSet<String> sourceAllFiles = new HashSet<String>();
		if (!srcPaths.isEmpty()) {
			for (String eachPath : srcPaths) {
				sourceAllFiles.addAll(getListFiles(ftpClient, eachPath, parentLevel, maxTraversalLevel));
			}
		}
		return sourceAllFiles;
	}

}
