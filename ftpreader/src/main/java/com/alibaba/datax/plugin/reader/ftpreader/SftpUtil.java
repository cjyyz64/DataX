package com.alibaba.datax.plugin.reader.ftpreader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.ftpreader.FtpReader.Job;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

/**
 * 
 * @ClassName: SftpUtil
 * @Description: 通过sftp协议连接ftp服务器的工具类
 * @author hanfa.shf@alibaba-inc.com
 * @date 2015年7月6日 下午4:25:50
 *
 */
public class SftpUtil {
	private static final Logger LOG = LoggerFactory.getLogger(Job.class);

	Session session = null;
	Channel channel = null;
	
	/**
	 * 
	* @Title: getChannel 
	* @Description: 通过sftp协议与ftp服务器建立连接 
	* @param @param host
	* @param @param username
	* @param @param password
	* @param @param port
	* @param @param timeout
	* @param @return     
	* @return ChannelSftp 返回连接了ftp服务器的手柄
	* @throws
	 */
	public ChannelSftp getChannel(String host, String username, String password, int port, int timeout) {
		JSch jsch = new JSch(); // 创建JSch对象
		try {
			session = jsch.getSession(username, host, port);
			// 根据用户名，主机ip，端口获取一个Session对象
			// 如果服务器连接不上，则抛出异常
			if (session == null) {
				throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN,
						"session is null,无法通过sftp与服务器建立链接，请检查主机名和用户名是否正确.");
			}

			session.setPassword(password); // 设置密码
			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config); // 为Session对象设置properties
			session.setTimeout(timeout); // 设置timeout时间
			session.connect(); // 通过Session建立链接

			channel = session.openChannel("sftp"); // 打开SFTP通道
			channel.connect(); // 建立SFTP通道的连接
		} catch (JSchException e) {
			if(null != e.getCause()){
				String cause = e.getCause().toString();
				String unknownHostException = "java.net.UnknownHostException: " + host;
				String illegalArgumentException = "java.lang.IllegalArgumentException: port out of range:" + port;
				String wrongPort = "java.net.ConnectException: Connection refused";
				if (unknownHostException.equals(cause)) {
					String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
				} else if (illegalArgumentException.equals(cause) || wrongPort.equals(cause) ) {
					String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
				}
			}else {
				if("Auth fail".equals(e.getMessage())){
					String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
							"message:host =" + host + ",username = " + username + ",port =" + port);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message);
				}else{
					String message = String.format("与ftp服务器建立连接失败 : [%s]",
							"message:host =" + host + ",username = " + username + ",port =" + port);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
				}				
			}
		}

		return (ChannelSftp) channel;
	}

	/**
	 * 
	* @Title: closeChannel 
	* @Description: 与ftp服务器断开连接
	* @param @param sftp  连接了ftp服务器的手柄
	* @return void 
	* @throws
	 */
	public void closeChannel(ChannelSftp sftp) {
		if (sftp != null) {
			sftp.disconnect();
		}
		if (session != null) {
			session.disconnect();
		}
	}
	
	/**
	 * 
	* @Title: isDirExist 
	* @Description: 判断指定路径的目录是否存在
	* @param @param sftp 连接了ftp服务器的手柄
	* @param @param directory
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public boolean isDirExist(ChannelSftp sftp, String directory) {
		try {
			SftpATTRS sftpATTRS = sftp.lstat(directory);
			return sftpATTRS.isDir();
		} catch (SftpException e) {
			if (e.getMessage().toLowerCase().equals("no such file")) {
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", directory);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
			}
			String message = String.format("进入目录：[%s]时发生I/O异常,请确认与ftp服务器的连接正常", directory);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
		}
	}

	/**
	 * 
	* @Title: isFileExist 
	* @Description: 判断指定路径的文件是否存在，通过文件大小判断
	* @param @param sftp 连接了ftp服务器的手柄
	* @param @param srcSftpFilePath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public boolean isFileExist(ChannelSftp sftp, String srcSftpFilePath) {
		boolean isExitFlag = false;
		// 文件大于等于0则存在文件
		long filesize = getFileSize(sftp, srcSftpFilePath);
		if (filesize >= 0) {
			isExitFlag = true;
		}
		return isExitFlag;
	}
	
	/**
	 * 
	* @Title: isLink 
	* @Description: 判断指定路径的文件是否是链接
	* @param @param sftp
	* @param @param srcSftpFilePath
	* @param @return     
	* @return boolean 
	* @throws
	 */
	public boolean isLink(ChannelSftp sftp, String srcSftpFilePath) {
		try {
			SftpATTRS sftpATTRS = sftp.lstat(srcSftpFilePath);
			return sftpATTRS.isLink();
		} catch (SftpException e) {
			if (e.getMessage().toLowerCase().equals("no such file")) {
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", srcSftpFilePath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
			} else {
				String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", srcSftpFilePath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
			}
		}
	}
	
	/**
	 * 
	* @Title: getFileSize 
	* @Description: 获取指定路径文件的大小 
	* @param @param sftp 连接了ftp服务器的手柄
	* @param @param srcSftpFilePath
	* @param @return     
	* @return long 
	* @throws
	 */
	public long getFileSize(ChannelSftp sftp, String srcSftpFilePath) {
		long filesize = 0;// 文件大于等于0则存在
		try {
			SftpATTRS sftpATTRS = sftp.lstat(srcSftpFilePath);
			filesize = sftpATTRS.getSize();
		} catch (SftpException e) {
			filesize = -1;// 获取文件大小异常
			if (e.getMessage().toLowerCase().equals("no such file")) {
				filesize = -2;// 文件不存在
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", srcSftpFilePath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
			} else {
				String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", srcSftpFilePath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
			}
		}
		return filesize;
	}

	/**
	 * 
	* @Title: getListFiles 
	* @Description: 获取指定路径下符合条件的所有文件的绝对路径  
	* @param @param sftp 连接了ftp服务器的手柄
	* @param @param eachPath 指定路径
	* @param @param parentLevel 父目录的递归层数（首次为0）
	* @param @param maxTraversalLevel 允许的最大递归层数
	* @param @return     
	* @return HashSet<String> 
	* @throws
	 */
	HashSet<String> sourceFiles = new HashSet<String>();
	public HashSet<String> getListFiles(ChannelSftp sftp, String eachPath, int parentLevel, int maxTraversalLevel) {
		if(parentLevel < maxTraversalLevel){
			String parentPath = null;// 父级目录,以'/'结尾
			int pathLen = eachPath.length();
			if (eachPath.contains("*") || eachPath.contains("?")) {//*和？的限制
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
				if (isDirExist(sftp, subPath)) {
					parentPath = subPath;
				} else {
					String message = String.format("不能进入目录：[%s]," + "请确认您的配置项path:[%s]存在，且配置的用户有权限进入", subPath,
							eachPath);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
				}
	
			} else if (isDirExist(sftp, eachPath)) {
				// path是目录
				if (eachPath.charAt(pathLen - 1) == IOUtils.DIR_SEPARATOR) {
					parentPath = eachPath;
				} else {
					parentPath = eachPath + IOUtils.DIR_SEPARATOR;
				}
			} else if(isLink(sftp, eachPath)){
				//path是链接文件
				String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", eachPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
			}else if (isFileExist(sftp, eachPath)) {
				// path指向具体文件
				sourceFiles.add(eachPath);
				return sourceFiles;
			} else {
				String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", eachPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
			}
	
			try {
				Vector vector = sftp.ls(eachPath);
				for (int i = 0; i < vector.size(); i++) {
					LsEntry le = (LsEntry) vector.get(i);
					String strName = le.getFilename();
					String filePath = parentPath + strName;
	
					if (isDirExist(sftp, filePath)) {
						// 是子目录
						if (!(strName.equals(".") || strName.equals(".."))) {
							//递归处理
							getListFiles(sftp, filePath, parentLevel+1, maxTraversalLevel);
						}
					} else if(isLink(sftp, filePath)){
						//是链接文件
						String message = String.format("文件:[%s]是链接文件，当前不支持链接文件的读取", filePath);
						LOG.error(message);
						throw DataXException.asDataXException(FtpReaderErrorCode.LINK_FILE, message);
					}else if (isFileExist(sftp, filePath)) {
						// 是文件
						sourceFiles.add(filePath);
					} else {
						String message = String.format("请确认path:[%s]存在，且配置的用户有权限读取", filePath);
						LOG.error(message);
						throw DataXException.asDataXException(FtpReaderErrorCode.FILE_NOT_EXISTS, message);
					}
	
				} // end for vector
			} catch (SftpException e) {
				String message = String.format("获取path：[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常", eachPath);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
			}
			
			return sourceFiles;
		}else{
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
	* @param @param sftp
	* @param @param srcPaths
	* @param @param parentLevel
	* @param @param maxTraversalLevel
	* @param @return     
	* @return HashSet<String> 
	* @throws
	 */
	public HashSet<String> getAllFiles(ChannelSftp sftp, List<String> srcPaths, int parentLevel, int maxTraversalLevel){
		HashSet<String> sourceAllFiles = new HashSet<String>();
		if (!srcPaths.isEmpty()) {
			for (String eachPath : srcPaths) {
				sourceAllFiles.addAll(getListFiles(sftp, eachPath, parentLevel, maxTraversalLevel));
			}
		}
		return sourceAllFiles;
	}
	
}
