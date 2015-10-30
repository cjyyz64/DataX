package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;

public  class HdfsHelper {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsWriter.Job.class);
    public FileSystem fileSystem = null;
    public JobConf conf = null;

    public void getFileSystem(String defaultFS){
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);
        conf = new JobConf(hadoopConf);
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            String message = String.format("获取FileSystem时发生网络IO异常,请检查您的网络是否正常!HDFS地址：[%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }catch (Exception e) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }

        if(null == fileSystem || null == conf){
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS =" + defaultFS);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }

    /**
     *获取指定目录先的文件列表
     * @param dir
     * @return
     * 拿到的是文件全路径，
     * eg：hdfs://10.101.204.12:9000/user/hive/warehouse/writer.db/text/test.textfile
     */
    public String[] hdfsDirList(String dir){
        Path path = new Path(dir);
        String[] files = null;
        try {
            FileStatus[] status = fileSystem.listStatus(path);
            files = new String[status.length];
            for(int i=0;i<status.length;i++){
                files[i] = status[i].getPath().toString();
            }
        } catch (IOException e) {
            String message = String.format("获取目录[%s]文件列表时发生网络IO异常,请检查您的网络是否正常！", dir);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
        return files;
    }

    /**
     * 获取以fileName__ 开头的文件列表
     * @param dir
     * @param fileName
     * @return
     */
    public Path[] hdfsDirList(String dir,String fileName){
        Path path = new Path(dir);
        Path[] files = null;
        String filterFileName = fileName + "__*";
        try {
            PathFilter pathFilter = new GlobFilter(filterFileName);
            FileStatus[] status = fileSystem.listStatus(path,pathFilter);
            files = new Path[status.length];
            for(int i=0;i<status.length;i++){
                files[i] = status[i].getPath();
            }
        } catch (IOException e) {
            String message = String.format("获取目录[%s]下文件名以[%s]开头的文件列表时发生网络IO异常,请检查您的网络是否正常！",
                    dir,fileName);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
        return files;
    }

    public boolean isPathexists(String filePath) {
        Path path = new Path(filePath);
        boolean exist = false;
        try {
            exist = fileSystem.exists(path);
        } catch (IOException e) {
            String message = String.format("判断文件路径[%s]是否存在时发生网络IO异常,请检查您的网络是否正常！",
                    "message:filePath =" + filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
        return exist;
    }

    public boolean isPathDir(String filePath) {
        Path path = new Path(filePath);
        boolean isDir = false;
        try {
            isDir = fileSystem.isDirectory(path);
        } catch (IOException e) {
            String message = String.format("判断路径[%s]是否是目录时发生网络IO异常,请检查您的网络是否正常！", filePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
        return isDir;
    }

    public void deleteFiles(Path[] paths){
        for(int i=0;i<paths.length;i++){
            LOG.info(String.format("delete file [%s].", paths[i].toString()));
            try {
                fileSystem.delete(paths[i],true);
            } catch (IOException e) {
                String message = String.format("删除文件[%s]时发生IO异常,请检查您的网络是否正常！",
                        paths[i].toString());
                LOG.error(message);
                throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
            }
        }
    }

    //关闭FileSystem
    public void closeFileSystem(){
        try {
            fileSystem.close();
        } catch (IOException e) {
            String message = String.format("关闭FileSystem时发生IO异常,请检查您的网络是否正常！");
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.CONNECT_HDFS_IO_ERROR, message);
        }
    }


    //textfile格式文件
    public  FSDataOutputStream getOutputStream(String path){
        Path storePath = new Path(path);
        FSDataOutputStream fSDataOutputStream = null;
        try {
            fSDataOutputStream = fileSystem.create(storePath);
        } catch (IOException e) {
            String message = String.format("Create an FSDataOutputStream at the indicated Path[%s] failed: [%s]",
                    "message:path =" + path);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, message);
        }
        return fSDataOutputStream;
    }

    /**
     * 写textfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void textFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                   TaskPluginCollector taskPluginCollector){
        String actuallyFileName =fileName;
        String encoding = config.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
        // handle blank encoding
        if (StringUtils.isBlank(encoding)) {
            LOG.warn(String.format("您配置的encoding为[%s], 使用默认值[%s]", encoding, Constant.DEFAULT_ENCODING));
            encoding = Constant.DEFAULT_ENCODING;
        }
        String compress = config.getString(Key.COMPRESS,null);
        String codecClassName = null;

        if(null == compress){
            codecClassName = null;
        }else if("gzip".equalsIgnoreCase(compress)){
            codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
            actuallyFileName = fileName + ".gz";
        }else if ("bzip2".equalsIgnoreCase(compress)) {
            codecClassName = "org.apache.hadoop.io.compress.BZip2Codec";
            actuallyFileName = fileName + ".bz2";
        }else {
            throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE,
                    String.format("目前TEXT FILE仅支持GZIP、BZIP2 两种压缩, 不支持您配置的 compress 模式 : [%s]",
                            compress));
        }
        BufferedWriter writer = null;
        try {
            FSDataOutputStream fSDataOutputStream = getOutputStream(actuallyFileName);
            if(codecClassName == null){
                writer = new BufferedWriter(new OutputStreamWriter(fSDataOutputStream, encoding));
            }else {
                Class<?> codecClass = Class.forName(codecClassName);
                CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
                CompressionOutputStream compressionOutputStream = codec.createOutputStream(fSDataOutputStream);
                writer = new BufferedWriter(new OutputStreamWriter(compressionOutputStream, encoding));
            }
            HdfsHelper.doWriteToStream(lineReceiver, writer, config, taskPluginCollector);

        } catch (ClassNotFoundException cnfe) {
            throw DataXException.asDataXException(
                    HdfsWriterErrorCode.WRITER_RUNTIME_EXCEPTION,
                    String.format("您配置的压缩方式类找不到 : [%]", codecClassName), cnfe);
        }catch (UnsupportedEncodingException uee) {
            throw DataXException.asDataXException(
                    HdfsWriterErrorCode.WRITER_FILE_WITH_CHARSET_ERROR,
                    String.format("不支持的编码格式 : [%]", encoding), uee);
        }catch (IOException e) {
            throw DataXException.asDataXException(
                    HdfsWriterErrorCode.Write_FILE_IO_ERROR,
                    String.format("流写入错误 : [%]", actuallyFileName), e);
        }catch (Exception e) {
            throw DataXException.asDataXException(
                    HdfsWriterErrorCode.WRITER_RUNTIME_EXCEPTION,
                    "运行时错误, 请联系我们", e);
        }finally {
            IOUtils.closeQuietly(writer);
        }
    }
    private static void doWriteToStream(RecordReceiver lineReceiver,
                                        BufferedWriter writer, Configuration config,
                                        TaskPluginCollector taskPluginCollector) throws IOException {
        String nullFormat = config.getString(Key.NULL_FORMAT);
        char fieldDelimiter = config.getChar(Key.FIELD_DELIMITER);
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        Record record = null;
        while ((record = lineReceiver.getFromReader()) != null) {
            MutablePair<String, Boolean> transportResult = transportOneRecord(record, nullFormat, fieldDelimiter, columns, taskPluginCollector);
            if (!transportResult.getRight()) {
                writer.write(transportResult.getLeft());
            }
        }
    }

    public static MutablePair<String, Boolean> transportOneRecord(
            Record record, String nullFormat, char fieldDelimiter, List<Configuration> columnsConfiguration, TaskPluginCollector taskPluginCollector) {
        if (null == nullFormat) {
            nullFormat = "null";
        }
        MutablePair<List<Object>, Boolean> transportResultList =  transportOneRecord(record,nullFormat,columnsConfiguration,taskPluginCollector);

        MutablePair<String, Boolean> transportResult = new MutablePair<String, Boolean>();
        transportResult.setRight(false);
        if(null != transportResultList){
            String recordResult = StringUtils.join(transportResultList.getLeft(), fieldDelimiter) + IOUtils.LINE_SEPARATOR;
            transportResult.setRight(transportResultList.getRight());
            transportResult.setLeft(recordResult);
        }
        return transportResult;
    }

    /**
     * 写orcfile类型文件
     * @param lineReceiver
     * @param config
     * @param fileName
     * @param taskPluginCollector
     */
    public void orcFileStartWrite(RecordReceiver lineReceiver, Configuration config, String fileName,
                                  TaskPluginCollector taskPluginCollector){
        List<Configuration>  columns = config.getListConfiguration(Key.COLUMN);
        String nullFormat = config.getString(Key.NULL_FORMAT);

        List<String> columnNames = getColumnNames(columns);
        List<ObjectInspector> columnTypeInspectors = getColumnTypeInspectors(columns);
        StructObjectInspector inspector = (StructObjectInspector)ObjectInspectorFactory
                .getStandardStructObjectInspector(columnNames, columnTypeInspectors);

        OrcSerde orcSerde = getOrcSerde(config);

        //OutputFormat outFormat = new OrcOutputFormat();
        OrcOutputFormat outFormat = new OrcOutputFormat();
        try {
            RecordWriter writer = outFormat.getRecordWriter(fileSystem, conf, fileName, Reporter.NULL);
            Record record = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                MutablePair<List<Object>, Boolean> transportResult =  transportOneRecord(record,nullFormat,columns,taskPluginCollector);
                if (!transportResult.getRight()) {
                    writer.write(NullWritable.get(), orcSerde.serialize(transportResult.getLeft(), inspector));
                }
            }
            writer.close(Reporter.NULL);
        } catch (IOException e) {
            String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, message);
        }
    }

    public List<String> getColumnNames(List<Configuration> columns){
        List<String> columnNames = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            columnNames.add(eachColumnConf.getString(Key.NAME));
        }
        return columnNames;
    }

    /**
     * 根据writer配置的字段类型，构建inspector
     * @param columns
     * @return
     */
    public List<ObjectInspector>  getColumnTypeInspectors(List<Configuration> columns){
        List<ObjectInspector>  columnTypeInspectors = Lists.newArrayList();
        for (Configuration eachColumnConf : columns) {
            SupportHiveDataType columnType = SupportHiveDataType.valueOf(eachColumnConf.getString(Key.TYPE).toUpperCase());
            ObjectInspector objectInspector = null;
            switch (columnType) {
                case TINYINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case SMALLINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case INT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BIGINT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case FLOAT:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DOUBLE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case TIMESTAMP:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case DATE:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case STRING:
                case VARCHAR:
                case CHAR:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                case BOOLEAN:
                    objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    break;
                default:
                    throw DataXException
                            .asDataXException(
                                    HdfsWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                            eachColumnConf.getString(Key.NAME),
                                            eachColumnConf.getString(Key.TYPE)));
            }

            columnTypeInspectors.add(objectInspector);
        }
        return columnTypeInspectors;
    }

    public OrcSerde getOrcSerde(Configuration config){
        String fieldDelimiter = config.getString(Key.FIELD_DELIMITER);
        String compress = config.getString(Key.COMPRESS);
        String encoding = config.getString(Key.ENCODING);

        OrcSerde orcSerde = new OrcSerde();
        Properties properties = new Properties();
        properties.setProperty("orc.bloom.filter.columns", fieldDelimiter);
        properties.setProperty("orc.compress", compress);
        properties.setProperty("orc.encoding.strategy", encoding);

        orcSerde.initialize(conf, properties);
        return orcSerde;
    }

    public static MutablePair<List<Object>, Boolean> transportOneRecord(
            Record record, String nullFormat,List<Configuration> columnsConfiguration,
            TaskPluginCollector taskPluginCollector){

        if (null == nullFormat) {
            nullFormat = "null";
        }

        MutablePair<List<Object>, Boolean> transportResult = new MutablePair<List<Object>, Boolean>();
        transportResult.setRight(false);
        List<Object> recordList = Lists.newArrayList();
        int recordLength = record.getColumnNumber();
        if (0 != recordLength) {
            Column column;
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                if (null != column.getRawData()) {
                    boolean isDateColumn = column instanceof DateColumn;
                    SupportHiveDataType columnType = SupportHiveDataType.valueOf(
                            columnsConfiguration.get(i).getString(Key.TYPE).toUpperCase());
                    if (!isDateColumn) {
                        //根据writer端类型配置做类型转换
                        try {
                            switch (columnType) {
                                case TINYINT:
                                    recordList.add(Byte.valueOf(column.getRawData().toString()));
                                    break;
                                case SMALLINT:
                                    recordList.add(Short.valueOf(column.getRawData().toString()));
                                    break;
                                case INT:
                                    recordList.add(Integer.valueOf(column.getRawData().toString()));
                                    break;
                                case BIGINT:
                                    recordList.add(Long.valueOf(column.getRawData().toString()));
                                    break;
                                case FLOAT:
                                    recordList.add(Float.valueOf(column.getRawData().toString()));
                                    break;
                                case DOUBLE:
                                    recordList.add(Double.valueOf(column.getRawData().toString()));
                                    break;
                                case STRING:
                                case VARCHAR:
                                case CHAR:
                                    recordList.add(String.valueOf(column.getRawData().toString()));
                                    break;
                                case BOOLEAN:
                                    recordList.add(Boolean.valueOf(column.getRawData().toString()));
                                    break;
                                default:
                                    throw DataXException
                                            .asDataXException(
                                                    HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                    String.format(
                                                            "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                            columnsConfiguration.get(i).getString(Key.NAME),
                                                            columnsConfiguration.get(i).getString(Key.TYPE)));
                            }
                        } catch (Exception e) {
                            // warn: 此处认为脏数据
                            String message = String.format(
                                    "字段类型转换错误：你目标字段为[%s]类型，实际字段值为[%s].",
                                    columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData().toString());
                            taskPluginCollector.collectDirtyRecord(record, message);
                            transportResult.setRight(true);
                            break;
                        }
                    }else{
                        //日期，未指定format
                        try{
                            if(columnType.equals(SupportHiveDataType.DATE)){
                                recordList.add(new java.sql.Date(column.asDate().getTime()));
                            }else if(columnType.equals(SupportHiveDataType.TIMESTAMP)){
                                recordList.add(new java.sql.Timestamp(column.asDate().getTime()));
                            }else{
                                throw DataXException
                                        .asDataXException(
                                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                String.format(
                                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                        columnsConfiguration.get(i).getString(Key.NAME),
                                                        columnsConfiguration.get(i).getString(Key.TYPE)));
                            }
                        }catch (Exception e) {
                            String message = String.format(
                                    "字段类型转换错误：你目标字段为[%s]类型，实际字段值为[%s].",
                                    columnsConfiguration.get(i).getString(Key.TYPE), column.getRawData().toString());
                            taskPluginCollector.collectDirtyRecord(record, message);
                            transportResult.setRight(true);
                            break;
                        }
                    }
                }else {
                    // warn: it's all ok if nullFormat is null
                    recordList.add(nullFormat);
                }
            }
        }
        transportResult.setLeft(recordList);
        return transportResult;
    }
}
