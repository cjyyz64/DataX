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

import java.io.InputStream;
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
        private String path = null;
        private String defaultFS = null;
        private String encoding = null;
        private HashSet<String> sourceFiles;
        private int maxTraversalLevel;
        private DFSUtil dfsUtil = null;

        @Override
        public void init() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常在这里对用户的配置进行校验：是否缺失必填项？有无错误值？有没有无关配置项？...
             * 并给出清晰的报错/警告提示。校验通常建议采用静态工具类进行，以保证本类结构清晰。
             */
            LOG.debug("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();
            dfsUtil = new DFSUtil();
            this.validate();
            LOG.debug("init() ok and end...");

        }

        private void validate(){
            path = this.readerOriginConfig.getNecessaryValue(Key.PATH, HdfsReaderErrorCode.PATH_NOT_FIND_ERROR);
            if (StringUtils.isBlank(path)) {
                throw DataXException.asDataXException(
                        HdfsReaderErrorCode.PATH_NOT_FIND_ERROR, "您需要指定 path");
            }

            defaultFS = this.readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS,
                                                    HdfsReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);
            if (StringUtils.isBlank(defaultFS)) {
                throw DataXException.asDataXException(
                        HdfsReaderErrorCode.PATH_NOT_FIND_ERROR, "您需要指定 defaultFS");
            }

            dfsUtil.readfile(path,defaultFS);

            this.maxTraversalLevel = this.readerOriginConfig.getInt(Key.MAXTRAVERSALLEVEL, Constant.DEFAULT_MAX_TRAVERSAL_LEVEL);
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
                        eachColumnConf
                                .getNecessaryValue(Key.TYPE, HdfsReaderErrorCode.REQUIRED_VALUE);
                        Integer columnIndex = eachColumnConf
                                .getInt(Key.INDEX);
                        String columnValue = eachColumnConf
                                .getString(Key.VALUE);

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
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
            LOG.debug("prepare()");
            this.sourceFiles = dfsUtil.getAllFiles(path,0,maxTraversalLevel);
            LOG.info(String.format("您即将读取的文件数为: [%s]", this.sourceFiles.size()));
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常采用工具静态类完成把 Job 配置切分成多个 Task 配置的工作。
             * 这里的 adviceNumber 是框架根据用户的同步速度的要求建议的切分份数，仅供参考，不是强制必须切分的份数。
             */
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
            return new ArrayList<Configuration>();
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
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
            LOG.debug("post()");
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常配合 Job 中的 post() 方法一起完成 Job 的资源释放。
             */
            LOG.debug("destroy()");
        }

    }

    public static class Task extends Reader.Task {

        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);
        private Configuration taskConfig;
        private List<String> sourceFiles;
        private String defaultFS;
        private DFSUtil dfsUtil = null;

        @Override
        public void init() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：此处通过对 taskConfig 配置的读取，进而初始化一些资源为 startRead()做准备。
             */
            this.taskConfig = super.getPluginJobConf();
            this.sourceFiles = this.taskConfig.getList(Constant.SOURCE_FILES, String.class);
            this.defaultFS = this.taskConfig.getNecessaryValue(Key.DEFAULT_FS,
                    HdfsReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);
            this.dfsUtil = new DFSUtil();
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之后的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
        }

        @Override
        public void startRead(RecordSender recordSender) {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：此处适当封装确保简洁清晰完成数据读取工作。
             */
            LOG.debug("read start");
            for (String sourceFile : this.sourceFiles) {
                LOG.info(String.format("reading file : [%s]", sourceFile));
                InputStream inputStream = null;

                inputStream = dfsUtil.getInputStream(sourceFile,defaultFS);

                UnstructuredStorageReaderUtil.readFromStream(inputStream, sourceFile, this.taskConfig,
                        recordSender, this.getTaskPluginCollector());
                recordSender.flush();
            }

            LOG.debug("end read source files...");
        }

        @Override
        public void post() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：如果 Task 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：通常配合Task 中的 post() 方法一起完成 Task 的资源释放。.
             */
        }

    }

}