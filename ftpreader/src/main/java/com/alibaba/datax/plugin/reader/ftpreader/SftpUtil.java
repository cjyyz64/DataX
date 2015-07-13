package com.alibaba.datax.plugin.reader.ftpreader;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.reader.ftpreader.FtpReader.Job;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * 
 * @ClassName: SftpUtil
 * @Description: TODO
 * @author hanfa.shf@alibaba-inc.com
 * @date 2015年7月6日 下午4:25:50
 *
 */
public class SftpUtil {
	private static final Logger LOG = LoggerFactory.getLogger(Job.class);
	
	Session session = null;
	Channel channel = null;
	
	private static class SftpUtilHolder {
        private static final SftpUtil  INSTANCE = new SftpUtil();
    }
    private SftpUtil(){}
    public static final SftpUtil getInstance() {
        return SftpUtilHolder.INSTANCE;
    }

	public ChannelSftp getChannel(String host, String username, String password, int port, int timeout)
			throws JSchException {
		JSch jsch = new JSch(); // 创建JSch对象
		session = jsch.getSession(username, host, port); // 根据用户名，主机ip，端口获取一个Session对象
		// 如果服务器连接不上，则抛出异常
		if (session == null) {
			throw DataXException.asDataXException(FtpReaderErrorCode.FAIL_CONNEECT_FTPSERVER,
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

		return (ChannelSftp) channel;
	}

	// 关闭sftp连接
	public void closeChannel(ChannelSftp sftp) {
		if (sftp != null) {
			sftp.disconnect();
		}
		if (session != null) {
			session.disconnect();
		}
	}

	// 判断目录是否存在
	public boolean isDirExist(ChannelSftp sftp, String directory) {
		boolean isDirExistFlag = false;
		try {
			SftpATTRS sftpATTRS = sftp.lstat(directory);
			isDirExistFlag = true;
			return sftpATTRS.isDir();
		} catch (SftpException e) {
			if (e.getMessage().toLowerCase().equals("no such file")) {
				isDirExistFlag = false;
				String message = String.format("目录不存在: [%s]", directory);
				LOG.error(message);
			}
			e.printStackTrace();
		}
		return isDirExistFlag;
	}

	// 判断文件是否存在
	public boolean isFileExist(ChannelSftp sftp, String srcSftpFilePath) {
		boolean isExitFlag = false;
		// 文件大于等于0则存在文件
		if (getFileSize(sftp, srcSftpFilePath) >= 0) {
			isExitFlag = true;
		}
		return isExitFlag;
	}

	// 获取文件大小
	public long getFileSize(ChannelSftp sftp, String srcSftpFilePath) {
		long filesize = 0;// 文件大于等于0则存在
		try {
			SftpATTRS sftpATTRS = sftp.lstat(srcSftpFilePath);
			filesize = sftpATTRS.getSize();
		} catch (SftpException e) {
			filesize = -1;// 获取文件大小异常
			if (e.getMessage().toLowerCase().equals("no such file")) {
				filesize = -2;// 文件不存在
				String message = String.format("文件不存在: [%s]", srcSftpFilePath);
				LOG.error(message);
			}
			e.printStackTrace();
		}
		return filesize;
	}

	// 获取路径下所有文件的绝对路径
	public void listFiles(ChannelSftp sftp, List<String> srcPaths, HashSet<String> sourceFiles) {
		if (!srcPaths.isEmpty()) {
			for (String eachPath : srcPaths) {
				String parentPath = null;// 父级目录,以'/'结尾
				int pathLen = eachPath.length();
				if (eachPath.contains("*") || eachPath.contains("?")) {
					// 处理正则表达式
					int mark = eachPath.lastIndexOf('/');
					parentPath = eachPath.substring(0, mark + 1);
				} else if (isDirExist(sftp, eachPath)) {
					// path是目录
					if (eachPath.charAt(pathLen - 1) == '/') {
						parentPath = eachPath;
					} else {
						parentPath = eachPath + "/";
					}
				} else {
					// path指向具体文件
					if (isFileExist(sftp, eachPath)) {
						// 是文件
						if (!sourceFiles.contains(eachPath)) {
							sourceFiles.add(eachPath);
						}
						continue;
					}
				}

				Vector vector;
				ArrayList<String> childDir = new ArrayList<String>();// 存放子目录
				try {
					vector = sftp.ls(eachPath);
					for (int i = 0; i < vector.size(); i++) {
						LsEntry le = (LsEntry) vector.get(i);
						String strName = le.getFilename();
						String filePath = parentPath + strName;

						if (isDirExist(sftp, filePath)) {
							// 是子目录
							if (!(strName.equals(".") || strName.equals(".."))) {
								childDir.add(filePath);
							}
						} else if (isFileExist(sftp, filePath)) {
							// 是文件
							if (!sourceFiles.contains(filePath)) {
								sourceFiles.add(filePath);
							}
						}

					} // end for vector
				} catch (SftpException e) {
					if (e.getMessage().toLowerCase().equals("no such file")) {
						String message = String.format("您设定的路径不存在: [%s]", eachPath);
						LOG.error(message);
					}
					e.printStackTrace();
				}

				// 处理子目录
				listFiles(sftp, childDir, sourceFiles);

			} // end for eachPath

		} // end if

	}

}
