package com.alibaba.datax.plugin.reader.hdfsreader;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsReader.Job.class);

    private org.apache.hadoop.conf.Configuration hadoopConf = null;

    public DFSUtil(String defaultFS){
        hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.defaultFS", defaultFS);
    }

    private HashSet<String> sourceHDFSAllFilesList = new HashSet<String>();

    public HashSet<String> getHDFSAllFiles(String hdfsPath){
        try {
            FileSystem hdfs = FileSystem.get(hadoopConf);
            //判断hdfsPath是否包含正则符号
            if(hdfsPath.contains("*") || hdfsPath.contains("?")){
                Path path = new Path(hdfsPath);
                FileStatus stats[] = hdfs.globStatus(path);
                for(FileStatus f : stats){
                    if(f.isFile()){
                        sourceHDFSAllFilesList.add(f.getPath().toString());
                    }
                    else if(f.isDirectory()){
                        getHDFSALLFiles_NO_Regex(f.getPath().toString(), hdfs);
                    }
                }
            }
            else{
                getHDFSALLFiles_NO_Regex(hdfsPath, hdfs);
            }
            return sourceHDFSAllFilesList;
        }catch (IOException e){
            String message = String.format("无法读取路径[%s]下的所有文件,请确认您的配置项path是否正确，且配置的用户有权限进入"
                    , hdfsPath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.PATH_CONFIG_ERROR, message);
        }
    }

    private HashSet<String> getHDFSALLFiles_NO_Regex(String path,FileSystem hdfs) throws IOException{

        // 获取要读取的文件的根目录
        Path listFiles = new Path(path);

        // 获取要读取的文件的根目录的所有二级子文件目录
        FileStatus stats[] = hdfs.listStatus(listFiles);

        for (FileStatus f : stats) {
            // 判断是不是目录，如果是目录，递归调用
            if (f.isDirectory()) {
                getHDFSALLFiles_NO_Regex(f.getPath().toString(),hdfs);
            } else {
                sourceHDFSAllFilesList.add(f.getPath().toString());
            }
        }
        return sourceHDFSAllFilesList;
    }

    public InputStream getInputStream(String filepath){
        InputStream inputStream = null;
        Path path = new Path(filepath);
        try{
            FileSystem fs = FileSystem.get(hadoopConf);
            inputStream = fs.open(path);
            return inputStream;
        }catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void orcFileRead(String sourceOrcFilePath, Configuration readerSliceConfig,
                            RecordSender recordSender, TaskPluginCollector taskPluginCollector){

        List<Configuration> columnConfigs = readerSliceConfig.getListConfiguration(Key.COLUMN);
        Integer columnIndexMax = -1;
        for (Configuration columnConfig : columnConfigs) {
            Integer columnIndex = columnConfig.getInt(Key.INDEX);
            if(columnIndex!=null && columnIndex<0 ){
                String message = String.format("您column中配置的index不能小于0，请修改为正确的index");
                LOG.error(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION, message);
            }else if(columnIndex!=null && columnIndex>columnIndexMax){
                columnIndexMax = columnIndex;
            }
        }
        String allColumns = "";
        String allColumnTypes = "";
        for(int i=0; i<=columnIndexMax; i++){
            allColumns += "col";
            allColumnTypes += "string";
            if(i!=columnIndexMax){
                allColumns += ",";
                allColumnTypes += ":";
            }
        }
        if(columnIndexMax>=0) {
            JobConf conf = new JobConf(hadoopConf);
            Path orcFilePath = new Path(sourceOrcFilePath);
            Properties p = new Properties();
            p.setProperty("columns", allColumns);
            p.setProperty("columns.types", allColumnTypes);
            try {
                OrcSerde serde = new OrcSerde();
                serde.initialize(conf, p);
                StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
                InputFormat<?, ?> in = new OrcInputFormat();
                FileInputFormat.setInputPaths(conf, orcFilePath.toString());
                InputSplit[] splits = in.getSplits(conf, 1);
                System.out.println("splits.length==" + splits.length);

                conf.set("hive.io.file.readcolumn.ids", "1");
                RecordReader reader = in.getRecordReader(splits[0], conf, Reporter.NULL);
                Object key = reader.createKey();
                Object value = reader.createValue();
                // 获取列信息
                List<? extends StructField> fields = inspector.getAllStructFieldRefs();
                System.out.println("fields.size():" + fields.size());
                List<Object> recordFields = null;
                while (reader.next(key, value)) {
                    recordFields = new ArrayList<Object>();
                    for(int i=0; i<=columnIndexMax; i++){
                        Object field = inspector.getStructFieldData(value, fields.get(i));
                        recordFields.add(field);
                    }
                    transportOneRecord(columnConfigs, recordFields, recordSender, taskPluginCollector);
                }
                reader.close();
            }catch (Exception e){
                String message = String.format("从orcfile文件路径[%s]中读取数据发生异常，请联系系统管理员。"
                        , sourceOrcFilePath);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
            }
        }
    }

    private Record transportOneRecord(List<Configuration> columnConfigs, List<Object> recordFields
                , RecordSender recordSender, TaskPluginCollector taskPluginCollector){
        Record record = recordSender.createRecord();
        Column columnGenerated = null;
        try {
            for (Configuration columnConfig : columnConfigs) {
                String columnType = columnConfig
                        .getNecessaryValue(Key.TYPE, HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION);
                Integer columnIndex = columnConfig.getInt(Key.INDEX);
                String columnConst = columnConfig.getString(Key.VALUE);

                String columnValue = null;

                //TODO 如果是结构体等其它类型，怎么处理
                if (null != columnIndex) {
                    if(null!=recordFields.get(columnIndex))
                        columnValue = recordFields.get(columnIndex).toString();
                }else{
                    columnValue = columnConst;
                }
                Type type = Type.valueOf(columnType.toUpperCase());
                // it's all ok if nullFormat is null
                /*if (columnValue.equals(nullFormat)) {
                    columnValue = null;
                }*/
                switch (type) {
                    case STRING:
                        columnGenerated = new StringColumn(columnValue);
                        break;
                    case LONG:
                        try {
                            columnGenerated = new LongColumn(columnValue);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format(
                                    "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                    "LONG"));
                        }
                        break;
                    case DOUBLE:
                        try {
                            columnGenerated = new DoubleColumn(columnValue);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format(
                                    "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                    "DOUBLE"));
                        }
                        break;
                    case BOOLEAN:
                        try {
                            columnGenerated = new BoolColumn(columnValue);
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format(
                                    "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                    "BOOLEAN"));
                        }

                        break;
                    case DATE:
                        try {
                            if (columnValue == null) {
                                Date date = null;
                                columnGenerated = new DateColumn(date);
                            } else {
                                String formatString = columnConfig.getString(Key.FORMAT);
                                //if (null != formatString) {
                                if (StringUtils.isNotBlank(formatString)) {
                                    // 用户自己配置的格式转换
                                    SimpleDateFormat format = new SimpleDateFormat(
                                            formatString);
                                    columnGenerated = new DateColumn(
                                            format.parse(columnValue));
                                } else {
                                    // 框架尝试转换
                                    columnGenerated = new DateColumn(
                                            new StringColumn(columnValue)
                                                    .asDate());
                                }
                            }
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format(
                                    "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                    "DATE"));
                        }
                        break;
                    default:
                        String errorMessage = String.format(
                                "您配置的列类型暂不支持 : [%s]", columnType);
                        LOG.error(errorMessage);
                        throw DataXException
                                .asDataXException(
                                        UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                        errorMessage);
                }

                record.addColumn(columnGenerated);

            }
            recordSender.sendToWriter(record);
        } catch (IllegalArgumentException iae) {
            taskPluginCollector
                    .collectDirtyRecord(record, iae.getMessage());
        } catch (IndexOutOfBoundsException ioe) {
            taskPluginCollector
                    .collectDirtyRecord(record, ioe.getMessage());
        } catch (Exception e) {
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
            // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }

        return record;
    }

    private enum Type {
        STRING, LONG, BOOLEAN, DOUBLE, DATE, ;
    }

    /*public void readfile(String filepath,String defaultFS){

        Path path = new Path(filepath);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", defaultFS);
        try{
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }*/
}
