package com.alibaba.datax.plugin.reader.hdfsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class HdfsReader extends Reader {

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     * Task类init-->prepare-->startRead-->post-->destroy
     * Task类init-->prepare-->startRead-->post-->destroy
     *
     * Job类post-->destroy
     * </pre>
     */
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration readerOriginConfig = null;
//        private String path = null;
        private String defaultFS = null;
        private String encoding = null;
        private HashSet<String> sourceFiles;
        private String specifiedFileType = null;
        private DFSUtil dfsUtil = null;
        private List<String> path = null;

        @Override
        public void init() {

            LOG.debug("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();
            dfsUtil = new DFSUtil(defaultFS);
            LOG.debug("init() ok and end...");

        }

        private void validate(){
            defaultFS = this.readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS,
                    HdfsReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);
            if (StringUtils.isBlank(defaultFS)) {
                throw DataXException.asDataXException(
                        HdfsReaderErrorCode.PATH_NOT_FIND_ERROR, "您需要指定 defaultFS");
            }

            /*path = this.readerOriginConfig.getNecessaryValue(Key.PATH, HdfsReaderErrorCode.PATH_NOT_FIND_ERROR);
            if (StringUtils.isBlank(path)) {
                throw DataXException.asDataXException(
                        HdfsReaderErrorCode.PATH_NOT_FIND_ERROR, "您需要指定 path");
            }*/

            //path check
            String pathInString = this.readerOriginConfig.getNecessaryValue(Key.PATH, HdfsReaderErrorCode.REQUIRED_VALUE);
            if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
                path = new ArrayList<String>();
                path.add(pathInString);
            } else {
                path = this.readerOriginConfig.getList(Key.PATH, String.class);
                if (null == path || path.size() == 0) {
                    throw DataXException.asDataXException(HdfsReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
                }
                for (String eachPath : path) {
                    if(!eachPath.startsWith("/")){
                        String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
                        LOG.error(message);
                        throw DataXException.asDataXException(HdfsReaderErrorCode.ILLEGAL_VALUE, message);
                    }
                }
            }


            specifiedFileType = this.readerOriginConfig.getString(Key.FILETYPE,null);
            if(!StringUtils.isBlank(specifiedFileType) &&
                    !specifiedFileType.equalsIgnoreCase("ORC") &&
                    !specifiedFileType.equalsIgnoreCase("TEXT")){
                String message = "HdfsReader插件目前只支持ORC和TEXT两种格式的文件," +
                        "如果您需要指定读取的文件类型，请将filetype选项的值配置为ORC或者TEXT";
                throw DataXException.asDataXException(
                        HdfsReaderErrorCode.FILE_TYPE_ERROR, message);
            }

            encoding = this.readerOriginConfig.getString(Key.ENCODING, "UTF-8");

            try {
                Charsets.toCharset(encoding);
            } catch (UnsupportedCharsetException uce) {
                throw DataXException.asDataXException(
                        HdfsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("不支持的编码格式 : [%s]", encoding), uce);
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        HdfsReaderErrorCode.ILLEGAL_VALUE,
                        String.format("运行配置异常 : %s", e.getMessage()), e);
            }

            // 检测是column 是否为 ["*"] 若是则填为空
            List<Configuration> column = this.readerOriginConfig
                    .getListConfiguration(Key.COLUMN);
            if (null != column
                    && 1 == column.size()
                    && ("\"*\"".equals(column.get(0).toString()) || "'*'"
                    .equals(column.get(0).toString()))) {
                readerOriginConfig
                        .set(Key.COLUMN, new ArrayList<String>());
            } else {
                // column: 1. index type 2.value type 3.when type is Data, may have format
                List<Configuration> columns = this.readerOriginConfig
                        .getListConfiguration(Key.COLUMN);

                if (null == columns || columns.size() == 0) {
                    throw DataXException.asDataXException(
                            HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "您需要指定 columns");
                }

                if (null != columns && columns.size() != 0) {
                    for (Configuration eachColumnConf : columns) {
                        eachColumnConf.getNecessaryValue(Key.TYPE, HdfsReaderErrorCode.REQUIRED_VALUE);
                        Integer columnIndex = eachColumnConf.getInt(Key.INDEX);
                        String columnValue = eachColumnConf.getString(Key.VALUE);

                        if (null == columnIndex && null == columnValue) {
                            throw DataXException.asDataXException(
                                    HdfsReaderErrorCode.NO_INDEX_VALUE,
                                    "由于您配置了type, 则至少需要配置 index 或 value");
                        }

                        if (null != columnIndex && null != columnValue) {
                            throw DataXException.asDataXException(
                                    HdfsReaderErrorCode.MIXED_INDEX_VALUE,
                                    "您混合配置了index, value, 每一列同时仅能选择其中一种");
                        }

                    }
                }
            }

        }

        @Override
        public void prepare() {

            LOG.debug("prepare()");
            this.sourceFiles = dfsUtil.getAllFiles(path,specifiedFileType);
            LOG.info(String.format("您即将读取的文件数为: [%s]", this.sourceFiles.size()));
            LOG.info("待读取的所有文件路径如下：");
            for(String filePath :sourceFiles){
                LOG.info(String.format("[%s]", filePath));
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {

            LOG.debug("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            // warn:每个slice拖且仅拖一个文件,
            // int splitNumber = adviceNumber;
            int splitNumber = this.sourceFiles.size();
            if (0 == splitNumber) {
                throw DataXException.asDataXException(HdfsReaderErrorCode.EMPTY_DIR_EXCEPTION,
                        String.format("未能找到待读取的文件,请确认您的配置项path: %s", this.readerOriginConfig.getString(Key.PATH)));
            }

            List<List<String>> splitedSourceFiles = this.splitSourceFiles(new ArrayList(this.sourceFiles), splitNumber);
            for (List<String> files : splitedSourceFiles) {
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(Constant.SOURCE_FILES, files);
                readerSplitConfigs.add(splitedConfig);
            }

            return readerSplitConfigs;
        }


        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }


        @Override
        public void post() {

            LOG.debug("post()");
        }

        @Override
        public void destroy() {

            LOG.debug("destroy()");
        }

    }

    public static class Task extends Reader.Task {

        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);
        private Configuration taskConfig;
        private List<String> sourceFiles;
        private String defaultFS;
        private HdfsFileType fileType;
        private String encoding;
        private DFSUtil dfsUtil = null;

        @Override
        public void init() {

            this.taskConfig = super.getPluginJobConf();
            this.sourceFiles = this.taskConfig.getList(Constant.SOURCE_FILES, String.class);
            this.defaultFS = this.taskConfig.getNecessaryValue(Key.DEFAULT_FS,
                    HdfsReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);
            this.encoding = this.taskConfig.getString(Key.ENCODING, "UTF-8");
            this.dfsUtil = new DFSUtil(defaultFS);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startRead(RecordSender recordSender) {

            LOG.debug("read start");
            for (String sourceFile : this.sourceFiles) {
                LOG.info(String.format("reading file : [%s]", sourceFile));
                fileType = dfsUtil.checkHdfsFileType(sourceFile);

                if(fileType.equals(HdfsFileType.TEXT)
                        || fileType.equals(HdfsFileType.COMPRESSED_TEXT)) {

                    BufferedReader bufferedReader = dfsUtil.getBufferedReader(sourceFile, fileType, encoding);
                    UnstructuredStorageReaderUtil.doReadFromStream(bufferedReader, sourceFile,
                            this.taskConfig, recordSender, this.getTaskPluginCollector());
                }
                else if(fileType.equals(HdfsFileType.ORC)){

                    dfsUtil.orcFileStartRead(sourceFile, this.taskConfig,
                            recordSender, this.getTaskPluginCollector());
                }

                if(recordSender != null)
                    recordSender.flush();
            }

            LOG.debug("end read source files...");
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }

    }

}