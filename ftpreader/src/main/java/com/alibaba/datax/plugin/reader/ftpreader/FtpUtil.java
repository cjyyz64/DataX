package com.alibaba.datax.plugin.reader.ftpreader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.ftpreader.FtpReader.Job;

public class FtpUtil {
	private static final Logger LOG = LoggerFactory.getLogger(Job.class);
	private static class FtpUtilHolder {
        private static final FtpUtil  INSTANCE = new FtpUtil();
    }
    private FtpUtil(){}
    public static final FtpUtil getInstance() {
        return FtpUtilHolder.INSTANCE;
    }
    
	// 建立ftp连接
	public FTPClient connectServer(String host, String username, String password, int port, int timeout, String connectMode) {
		FTPClient ftpClient = new FTPClient();
		try {
			// 连接
			ftpClient.connect(host, port);
			// 登录
			ftpClient.login(username, password);
			ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
			ftpClient.setConnectTimeout(timeout);
			if("PASV".equals(connectMode)){
				ftpClient.enterRemotePassiveMode();
				ftpClient.enterLocalPassiveMode();
			}else if("PORT".equals(connectMode)){
				ftpClient.enterLocalActiveMode();
				//ftpClient.enterRemoteActiveMode(host, port);
			}			
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
				String message = String.format("与ftp服务器建立连接失败 : [%s]",
						"Connected failed to ftp server by ftp, message:host =" + host + ",username = " + username
								+ ",password = " + password + ",port =" + port);
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message);			
			}
		} catch (Exception e) {
			String message = String.format("与ftp服务器建立连接失败 : [%s]",
					"Connected failed to ftp server by ftp, message:host =" + host + ",username = " + username
							+ ",password = " + password + ",port =" + port);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_LOGIN, message, e);
		}
		return ftpClient;
	}

	// 关闭ftp连接
	public void closeServer(FTPClient ftpClient) {
		if (ftpClient.isConnected()) {
			try {
				ftpClient.logout();
				ftpClient.disconnect();
			} catch (IOException e) {
				String message = String.format("与ftp服务器断开连接失败 : [%s]",
						"Disconnect failed to ftp server by ftp");
				LOG.error(message);
				throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_DISCONNECT, message, e);
			}
		}
	}
	
	//判断目录是否存在
	public boolean isDirExist(FTPClient ftpClient, String directory) {
		try {
			return ftpClient.changeWorkingDirectory(directory);
		} catch (IOException e1) {
			String message = String.format("改变工作目录失败 : [%s]",
					"ftpClient.changeWorkingDirectory(directory),	message:"+ directory);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_ERROR, message, e1);
		}
	}

	//判断文件是否存在
	public boolean isFileExist(FTPClient ftpClient, String srcSftpFilePath)  {
		boolean isExitFlag = false;		
		try {
			FTPFile[] ftpFiles = ftpClient.listFiles(srcSftpFilePath);
			if(ftpFiles.length==1){
				isExitFlag = true;
			}
		} catch (IOException e) {
			String message = String.format("获取文件列表失败 : [%s]",
					"ftpClient.listFiles(srcSftpFilePath),	message:"+ srcSftpFilePath);
			LOG.error(message);
			throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_ERROR, message, e);
		}
		return isExitFlag;
	}
	
	// 获取路径下所有文件的绝对路径
	public void listFiles(FTPClient ftpClient,List<String> srcPaths ,HashSet<String> sourceFiles){
		if( !srcPaths.isEmpty()){
			for (String eachPath : srcPaths) {
				String parentPath = null;//父级目录,以'/'结尾
				int pathLen=eachPath.length();
				if(eachPath.contains("*") || eachPath.contains("?")){
					//处理正则表达式
					int mark=eachPath.lastIndexOf('/');
					parentPath=eachPath.substring(0, mark+1);					
				}else if(isDirExist(ftpClient,eachPath)){
					//path是目录
					if(eachPath.charAt(pathLen-1) == '/'){
						parentPath=eachPath;
					}else{
						parentPath=eachPath+"/";
					}
				}else{
					//path指向具体文件
					if(isFileExist(ftpClient,eachPath)){
						//是文件						
						if(!sourceFiles.contains(eachPath)){
							sourceFiles.add(eachPath);
						}
						continue;
					}
				}
				
				try {
					FTPFile[] fs = ftpClient.listFiles(eachPath);
					ArrayList<String> childDir=new ArrayList<String>();//存放子目录
					for (FTPFile ff : fs) {
						String strName = ff.getName();					
						String filePath=parentPath + strName;	
						if(ff.isDirectory()){
							if (!(strName.equals(".") || strName.equals(".."))) {
								childDir.add(filePath);
							}
						}else if(ff.isFile()){
							//是文件
							if(!sourceFiles.contains(filePath)){
								sourceFiles.add(filePath);
							}
						}
					}// end for FTPFile				
					//处理子目录
					listFiles(ftpClient,childDir,sourceFiles);
				} catch (IOException e) {
					String message = String.format("获取文件列表失败 : [%s]",
							"ftpClient.listFiles(eachPath),	message:"+ eachPath);
					LOG.error(message);
					throw DataXException.asDataXException(FtpReaderErrorCode.COMMAND_FTP_IO_ERROR, message, e);
				}
								
			}//end for eachPath
		}//end if
	}
	
}
