package com.alibaba.datax.plugin.reader.ftpreader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

public class FtpUtil {

	private static class FtpUtilHolder {
        private static final FtpUtil  INSTANCE = new FtpUtil();
    }
    private FtpUtil(){}
    public static final FtpUtil getInstance() {
        return FtpUtilHolder.INSTANCE;
    }
    
	// 建立ftp连接
	public FTPClient connectServer(String host, String username, String password, int port, int timeout) {
		FTPClient ftpClient = new FTPClient();
		try {
			// 连接
			ftpClient.connect(host, port);
			// 登录
			ftpClient.login(username, password);
			ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));

			// ftpClient.enterRemotePassiveMode();
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				System.out.println("连接失败");
				ftpClient.disconnect();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				e.printStackTrace();
			}
		}
	}
	
	//判断目录是否存在
	public boolean isDirExist(FTPClient ftpClient, String directory) {
		boolean isDirExistFlag = false;
		try {
			return ftpClient.changeWorkingDirectory(directory);
		} catch (IOException e1) {
			isDirExistFlag = false;
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return isDirExistFlag;
	}

	//判断文件是否存在
	public boolean isFileExist(FTPClient ftpClient, String srcSftpFilePath)  {
		boolean isExitFlag = false;		
		try {
			//ftpClient.completePendingCommand();
			FTPFile[] ftpFiles = ftpClient.listFiles(srcSftpFilePath);
			if(ftpFiles.length==1){
				isExitFlag = true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
								
			}//end for eachPath
		}//end if
	}
	
}
