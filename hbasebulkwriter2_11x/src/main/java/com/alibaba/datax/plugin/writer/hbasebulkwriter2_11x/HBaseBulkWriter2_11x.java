package com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.conf.DynamicColumnConf;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.conf.FixColumnConf;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2_11x.conf.HBaseJobParameterConf;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

@SuppressWarnings("Duplicates")
public class HBaseBulkWriter2_11x extends Writer {

    public static final String DATAX_HOME = System.getProperty("datax.home", "/home/admin/datax3");
    public static final String PYTHON_HOME = System.getenv("PYTHON_HOME");
    public static final String PLUGIN_HOME = DATAX_HOME + "/plugin/writer/hbasebulkwriter2_11x";
    public static final String ODPS_SORT_SCRIPT = PLUGIN_HOME + "/datax_odps_hbase_sort.py";
    public static final String ODPS_CLEAR_SCRIPT = PLUGIN_HOME + "/datax_odps_hbase_clear.py";
    private static String pythonCmd = "python";

    static {

        if (!Strings.isNullOrEmpty(PYTHON_HOME)) {
            if (PYTHON_HOME.endsWith("/")) {
                pythonCmd = PYTHON_HOME + "python";
            } else {
                pythonCmd = PYTHON_HOME + "/python";
            }
        }
    }


    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(HBaseBulkWriter2_11x.class);


        private Configuration dataxConf;
        HBaseBulker bulker;

        //preHander 后需要生成的writer job conf parameter
        FixColumnConf fixColumnConf = new FixColumnConf();
        DynamicColumnConf dynamicColumnConf = new DynamicColumnConf();

        private void loadBulker() {
            bulker = new HBaseBulker();
        }

        @Override
        public void init() {
            dataxConf = getPluginJobConf();

            if (Strings.isNullOrEmpty(dataxConf.getString(Key.PREFIX_FIXED)) && Strings.isNullOrEmpty(dataxConf.getString(Key.PREFIX_DYNAMIC))) {
                throw DataXException.asDataXException(BulkWriterError.RUNTIME,
                        "DATAX FATAL! 没有生成有效的配置(缺少fixedcolumn or dynamiccolumn),请联系askdatax");
            }

            loadBulker();
            try {
                RetryUtil.executeWithRetry(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        return bulker.init(dataxConf);
                    }
                }, 10, 1, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(BulkWriterError.IO, "请联系hbase同学检查hdfs集群", e);
            }
        }


        @Override
        public void prepare() {
            bulker.prepare();
        }

        @Override
        public void destroy() {
            LOG.info("HBaseBulkWriter2被destroy!");
            try {
                bulker.finish();
            } catch (Exception e) {
                throw DataXException.asDataXException(BulkWriterError.IO, e);
            } finally {
                bulker.clearDir();
                clearOdpsTmpTable();
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> slaveConfigurations = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                slaveConfigurations.add(this.dataxConf.clone());
            }

            LOG.info("Writer split to {} parts.", slaveConfigurations.size());

            return slaveConfigurations;
        }

        @Override
        public void post() {
            bulker.post();
        }

        @Override
        public void preHandler(Configuration jobConfiguration) {

            LOG.info("================ HBaseBulkWriter Phase 1 preHandler starting... ================ ");

            Configuration readerOriginPluginConf = jobConfiguration.getConfiguration(Key.READER_PARAMETER);
            Configuration writerOriginPluginConf = jobConfiguration.getConfiguration(Key.WRITER_PARAMETER + "." + Key.PARAMETER_TYPE_ORIGIN);
            //"job.content[0].reader.parameter.column"
            List<String> odps_column = readerOriginPluginConf.getList(Key.KEY_COLUMN, String.class);

            if (1 == odps_column.size()
                    && "*".equals(odps_column.get(0))) {
                LOG.error("odpsColumn不能配置为*");
                throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "odpsColumn不能配置为*");

            }

            String hbase_rowkey = writerOriginPluginConf.getString(Key.KEY_HBASE_ROWKEY);
            String rowkey_type = writerOriginPluginConf.getString(Key.KEY_ROWKEY_TYPE);

            if (Strings.isNullOrEmpty(hbase_rowkey) && Strings.isNullOrEmpty(rowkey_type)) {
                LOG.error("hbaseRowkey和rowkeyType不能同时为空");
                throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING, "hbaseRowkey和rowkeyType不能同时为空");
            }

            if (!Strings.isNullOrEmpty(hbase_rowkey) && !Strings.isNullOrEmpty(rowkey_type)) {
                LOG.error("hbaseRowkey和rowkeyType不能同时配置");
                throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "hbaseRowkey和rowkeyType不能同时配置");
            }

            if (!Strings.isNullOrEmpty(writerOriginPluginConf.getString(Key.KEY_TIME_COL))
                    &&
                    !Strings.isNullOrEmpty(writerOriginPluginConf.getString(Key.KEY_START_TS))
                    ) {
                LOG.error("timeCol和startTs不能同时配置");
                throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "timeCol和startTs不能同时配置");
            }

            Map<String, String> configuration = writerOriginPluginConf.get(Key.KEY_HBASE_CONFIGURATION, Map.class);
            String configurationStr = writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIGURATION);

            if ((configuration == null || configuration.size() < 1)
                    && (Strings.isNullOrEmpty(writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIG)) || Strings.isNullOrEmpty(writerOriginPluginConf.getString(Key.KEY_HDFS_CONFIG)))) {
                throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                        "需要配置hbaseClusterName 或者 配置hbaseConfig以及hdfsConfig，2者必须其一.");
            }

            String sort_column = getSortColumn(odps_column, hbase_rowkey, rowkey_type);

            String odpsProject = readerOriginPluginConf.getString(Key.KEY_PROJECT);
            String odpsTable = readerOriginPluginConf.getString(Key.KEY_TABLE);


            List<String> partition = readerOriginPluginConf.getList(Key.KEY_PARTITION, String.class);

            String dstTable = writerOriginPluginConf.getString(Key.KEY_HBASE_TABLE);
            if (Strings.isNullOrEmpty(dstTable)) {
                throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                        "Missing config hbaseTable. 请检查配置.");
            }

            String hbaseXml = writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIG);
            //
            String clusterId = writerOriginPluginConf.getString(Key.KEY_CLUSTERID);

            String uuid = UUID.randomUUID().toString();
            String suffix = uuid.replace("-", "a");

            String real_odps_table = String.format("%s_%s_%s", Key.ODPS_TMP_TBL_PREFIX, odpsTable, suffix);

            if (real_odps_table.length() > 123) {
                suffix = uuid.substring(0, uuid.indexOf("-", 0));
                real_odps_table = String.format("%s_%s_%s", Key.ODPS_TMP_TBL_PREFIX, odpsTable, suffix);
            }

            if (real_odps_table.length() > 123) {
                LOG.error(String.format("odps 表名(%s)太长，导致中间表名(%s)可能会超过ODPS的限制128个字节，请缩短odps的table名(减少%s)", odpsTable, real_odps_table, real_odps_table.length() - 123));
                throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, String.format("odps 表名(%s)太长，导致中间表名(%s)可能会超过ODPS的限制128个字节，请缩短odps的table名(减少%s)", odpsTable, real_odps_table, real_odps_table.length() - 123));
            }

            String bucketNum = writerOriginPluginConf.getString(Key.KEY_OPTIONAL_BUCKETNUM);
            String dynamicQualifier = "false";
            if (Strings.isNullOrEmpty(hbase_rowkey) && !Strings.isNullOrEmpty(rowkey_type)) {
                dynamicQualifier = "true";
            }
            String accessId = readerOriginPluginConf.getString(Key.KEY_ACCESSID);
            String accessKey = readerOriginPluginConf.getString(Key.KEY_ACCESSKEY);

            if (Strings.isNullOrEmpty(accessId) || Strings.isNullOrEmpty(accessKey)) {
                throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "缺少odps的accessId和accessKey");
            }

            final List<String> cmdList = new ArrayList<String>();


            cmdList.add(pythonCmd);
            cmdList.add(ODPS_SORT_SCRIPT);
            cmdList.add("--project");
            cmdList.add(odpsProject);
            cmdList.add("--src_table");
            cmdList.add(odpsTable);
            cmdList.add("--sort_column");
            cmdList.add(sort_column);

            String parts = formatPartitions(partition);

            if (!Strings.isNullOrEmpty(parts) && !"*".equals(parts)) {
                cmdList.add("--partition");
                cmdList.add(parts);
            }

            cmdList.add("--dst_table");
            cmdList.add(dstTable);

            if (!Strings.isNullOrEmpty(hbaseXml)) {
                cmdList.add("--hbase_config");
                cmdList.add(hbaseXml);
            }

            if (!Strings.isNullOrEmpty(configurationStr)) {
                cmdList.add("--configuration");
                cmdList.add(configurationStr);
            }

            if (!Strings.isNullOrEmpty(clusterId)) {
                cmdList.add("--cluster_id");
                cmdList.add(clusterId);
            }

            cmdList.add("--suffix");
            cmdList.add(suffix);

            if (!Strings.isNullOrEmpty(bucketNum)) {
                cmdList.add("--bucket_num");
                cmdList.add(bucketNum);
            }
            cmdList.add("--dynamic_qualifier");
            cmdList.add(dynamicQualifier);

            if (!Strings.isNullOrEmpty(rowkey_type)) {
                cmdList.add("--rowkey_type");
                cmdList.add(rowkey_type);
            }

            cmdList.add("--access_id");
            cmdList.add(accessId);
            cmdList.add("--access_key");
            cmdList.add(accessKey);
            cmdList.add("--datax_home");
            cmdList.add(DATAX_HOME);

            try {
                RetryUtil.executeWithRetry(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        runSortScript(cmdList);
                        return 0;
                    }
                }, 10, 1, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(BulkWriterError.RUNTIME, ODPS_SORT_SCRIPT + " 运行异常 ，请检查日志，或者联系askdatax", e);
            }

            //change the origin configuration

            //for reader:
            jobConfiguration.set("job.content[0].reader.parameter.table", real_odps_table);
            jobConfiguration.set("job.content[0].reader.parameter.partition", Lists.newArrayList("datax_pt=*"));

            //for writer:


            HBaseJobParameterConf hBaseJobParameterConf;
            String mode;
            if ("true".equals(dynamicQualifier)) {
                mode = Key.PREFIX_DYNAMIC;
                hBaseJobParameterConf = getDynamicColumnConf(writerOriginPluginConf, suffix);
            } else {
                mode = Key.PREFIX_FIXED;
                hBaseJobParameterConf = getFixColumnConf(writerOriginPluginConf, suffix);
            }


            jobConfiguration.set("job.content[0].writer.parameter." + mode, hBaseJobParameterConf);

            LOG.info("================ HBaseBulkWriter Phase 1 preHandler finish... ================ ");

        }


        private void runSortScript(List<String> cmdList) throws IOException, InterruptedException {
            ProcessBuilder builder = new ProcessBuilder(cmdList);

            builder.redirectErrorStream(true);
            Process p = builder.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                LOG.info("ODPS_SORT_SCRIPT => " + line);
            }
            int resCode = p.waitFor();

            if (resCode != 0) {
                LOG.error("{} run failed, rescode={}", ODPS_SORT_SCRIPT, resCode);
                throw new IOException(ODPS_SORT_SCRIPT + "运行返回值不为0,resCode=" + resCode);
            }
        }

        public static String formatPartition(String partition) {
            if (StringUtils.isBlank(partition)) {
                return null;
            } else {
                return partition.trim().replaceAll(" *= *", "=")
                        .replaceAll(" */ *", ",").replaceAll(" *, *", ",")
                        .replaceAll("'", "");
            }
        }

        public static String formatPartitions(List<String> partitions) {
            if (null == partitions || partitions.isEmpty()) {
                return null;
            } else if(partitions.size()>1){
                throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL,
                        "配置的分区不能支持多个分区(不是多级分区).");
            }else {
                  return   formatPartition(partitions.get(0));
            }
        }

        private void clearOdpsTmpTable() {
            Configuration readerConf = getPeerPluginJobConf();
            if (readerConf == null) {
                return;
            }
            String odpsProject = readerConf.getString(Key.KEY_PROJECT);
            String odpsTable = readerConf.getString(Key.KEY_TABLE);
            String accessId = readerConf.getString(Key.KEY_ACCESSID);
            String accessKey = readerConf.getString(Key.KEY_ACCESSKEY);


            List<String> cmdList = new ArrayList<String>();
            cmdList.add(pythonCmd);
            cmdList.add(ODPS_CLEAR_SCRIPT);
            cmdList.add("--project");
            cmdList.add(odpsProject);
            cmdList.add("--table");
            cmdList.add(odpsTable);
            cmdList.add("--access_id");
            cmdList.add(accessId);
            cmdList.add("--access_key");
            cmdList.add(accessKey);

            if (Strings.isNullOrEmpty(odpsTable)
                    || Strings.isNullOrEmpty(odpsProject)
                    || Strings.isNullOrEmpty(accessId)
                    || Strings.isNullOrEmpty(accessKey)
                    || !odpsTable.startsWith(Key.ODPS_TMP_TBL_PREFIX)) {
                return;
            }


            ProcessBuilder builder = new ProcessBuilder(cmdList);


            builder.redirectErrorStream(true);
            try {
                Process p = builder.start();

                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    LOG.info("ODPS_CLEAR_SCRIPT => " + line);
                }
                int resCode = p.waitFor();

                if (resCode != 0) {
                    LOG.error("{} run failed, rescode={}", ODPS_CLEAR_SCRIPT, resCode);
                }

            } catch (Exception e) {
                LOG.error("{} run Exception {}", ODPS_CLEAR_SCRIPT, e.getMessage());
            }
        }

        private String getPartitions(List<String> partitions) {
            if (partitions == null || partitions.size() == 0) {
                return null;
            }

            ArrayList<String> resList = Lists.newArrayList(partitions);

            //去除空值
            for (String v : partitions) {
                if (Strings.isNullOrEmpty(v)) {
                    resList.remove(v);
                }
            }
            return Joiner.on("/").join(resList);
        }

        private FixColumnConf getFixColumnConf(Configuration writerOriginPluginConf, String suffix) {
            String hbase_column = writerOriginPluginConf.getString(Key.KEY_HBASE_COLUMN);

            String[] hbaseColumns = hbase_column.split(",");
            for (String hColumn : hbaseColumns) {
                String[] ele = hColumn.split("\\|");
                FixColumnConf.HbaseColumn hbaseColumn = new FixColumnConf.HbaseColumn();
                hbaseColumn.index = ele[0];
                hbaseColumn.htype = ele[1];
                hbaseColumn.hname = ele[2];
                fixColumnConf.getHbaseColumn().add(hbaseColumn);
            }

            fixColumnConf.hbaseTable = writerOriginPluginConf.getString(Key.KEY_HBASE_TABLE);
            //需要检查hdfs目录，确保目录唯一，否则更换为新的目录名
            fixColumnConf.hbaseOutput = getUniqHDFSDirName(writerOriginPluginConf, suffix, fixColumnConf.hbaseTable);
            fixColumnConf.hbaseConfig = writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIG);
            fixColumnConf.hdfsConfig = writerOriginPluginConf.getString(Key.KEY_HDFS_CONFIG);
            fixColumnConf.nullMode = writerOriginPluginConf.getString(Key.KEY_NULL_MODE);
            fixColumnConf.startTs = writerOriginPluginConf.getString(Key.KEY_START_TS);
            fixColumnConf.timeCol = writerOriginPluginConf.getString(Key.KEY_TIME_COL);
            fixColumnConf.bucketNum = writerOriginPluginConf.getString(Key.KEY_BUCKET_NUM);
            fixColumnConf.encoding = writerOriginPluginConf.getString(Key.KEY_ENCODING);
            fixColumnConf.configuration = (Map<String, String>) writerOriginPluginConf.get(Key.KEY_HBASE_CONFIGURATION, Map.class);

            return fixColumnConf;
        }

        private DynamicColumnConf getDynamicColumnConf(Configuration writerOriginPluginConf, String suffix) {
            String hbase_column = writerOriginPluginConf.getString(Key.KEY_HBASE_COLUMN);
            String[] hbaseColumns = hbase_column.split(",");
            for (String hColumn : hbaseColumns) {
                String[] ele = hColumn.split("\\|");
                DynamicColumnConf.HBaseRule hBaseRule = new DynamicColumnConf.HBaseRule();
                hBaseRule.pattern = ele[1];
                hBaseRule.htype = ele[0];
                dynamicColumnConf.getHbaseColumn().getRules().add(hBaseRule);
            }

            dynamicColumnConf.hbaseTable = writerOriginPluginConf.getString(Key.KEY_HBASE_TABLE);
            //需要检查hdfs目录，确保目录唯一，否则更换为新的目录名
            dynamicColumnConf.hbaseOutput = getUniqHDFSDirName(writerOriginPluginConf, suffix, dynamicColumnConf.hbaseTable);
            dynamicColumnConf.hbaseConfig = writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIG);
            dynamicColumnConf.hdfsConfig = writerOriginPluginConf.getString(Key.KEY_HDFS_CONFIG);
            dynamicColumnConf.rowkeyType = writerOriginPluginConf.getString(Key.KEY_ROWKEY_TYPE);
            dynamicColumnConf.nullMode = writerOriginPluginConf.getString(Key.KEY_NULL_MODE);
            dynamicColumnConf.startTs = writerOriginPluginConf.getString(Key.KEY_START_TS);
            dynamicColumnConf.timeCol = writerOriginPluginConf.getString(Key.KEY_TIME_COL);
            dynamicColumnConf.bucketNum = writerOriginPluginConf.getString(Key.KEY_BUCKET_NUM);
            dynamicColumnConf.encoding = writerOriginPluginConf.getString(Key.KEY_ENCODING);
            dynamicColumnConf.configuration = (Map<String, String>) writerOriginPluginConf.get(Key.KEY_HBASE_CONFIGURATION, Map.class);
            return dynamicColumnConf;
        }

        private String getUniqHDFSDirName(Configuration writerOriginPluginConf, String suffix, String hbaseTable) {
            final org.apache.hadoop.conf.Configuration conf = HBaseHelper.getConfiguration(
                    writerOriginPluginConf.getString(Key.KEY_HDFS_CONFIG),
                    writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIG),
                    writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIGURATION),
                    null);

            String configHDFSPath = writerOriginPluginConf.getString(Key.KEY_HBASE_OUTPUT);

            configHDFSPath = Strings.isNullOrEmpty(configHDFSPath) ? Key.HDFS_DIR_BULKLOAD : configHDFSPath;

            final String originDir = configHDFSPath + "/" + suffix + "_" + hbaseTable;
            String res ;
            try {
                res = RetryUtil.executeWithRetry(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return HBaseHelper.checkOutputDirAndMake(conf, originDir);
                    }
                }, 10, 1, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(BulkWriterError.RUNTIME, "获取hdfs目录失败，请联系hbase同学检查hdfs集群", e);
            }
            return res;
        }

        private String getSortColumn(List<String> odps_columns, String hbase_rowkey, String rowkey_type) {
            List<String> sort_column = Lists.newArrayList();

            if (!Strings.isNullOrEmpty(hbase_rowkey)) {
                //for fixcolumn
                String[] hbase_rowkeys = hbase_rowkey.split(",");

                for (String hbaseRow : hbase_rowkeys) {
                    String[] elem = hbaseRow.split("\\|");
                    int index = Integer.valueOf(elem[0]);
                    String typ = elem[1];
                    if (index != -1) {
                        if (index >= odps_columns.size()) {
                            LOG.error("固定列rowkey的index配置错误，不在odps_column的范围内:" + index + "=>" + odps_columns.size());
                            throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "固定列rowkey的index配置错误，不在odps_column的范围内:" + index + "=>" + odps_columns.size());
                        }
                        sort_column.add(typ + ":" + odps_columns.get(index));

                        //for fixColumnConf
                        FixColumnConf.RowkeyColumn hbaseRowkey = new FixColumnConf.RowkeyColumn();
                        hbaseRowkey.index = String.valueOf(index);
                        hbaseRowkey.htype = typ;

                        fixColumnConf.getHbaseRowkey().add(hbaseRowkey);

                    } else {
                        if (elem.length != 3) {
                            LOG.error("hbase rowkey的index为-1,则格式必须为-1|type|fixed");
                            throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "hbase rowkey的index为-1,则格式必须为-1|type|fixed");
                        }
                        String fixed = elem[2];
                        //for bug: when constant is String, '' is added for odps,but odps just is meta. for hbase '' is the real chart.
                        // so remove.
                        String afterFixed = fixed;
                        if (typ.equals("string")) {
                            afterFixed = "'" + fixed + "'";
                        }
                        sort_column.add(typ + ":" + afterFixed);

                        //for fixColumnConf
                        FixColumnConf.RowkeyColumn hbaseRowkey = new FixColumnConf.RowkeyColumn();
                        hbaseRowkey.index = String.valueOf(index);
                        hbaseRowkey.htype = typ;
                        hbaseRowkey.constant = fixed;

                        fixColumnConf.getHbaseRowkey().add(hbaseRowkey);
                    }
                }
            } else {
                //for dynamic column
                if (odps_columns.size() != 4 || Strings.isNullOrEmpty(rowkey_type)) {
                    LOG.error("动态列必须为4元组:rowkey,cf_qual,ts,val,且rowkey_type不能为空");
                    throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "动态列必须为4元组:rowkey,cf_qual,ts,val,且rowkey_type不能为空，请确认你的表是hbase的四元组表");
                }
                sort_column.add(rowkey_type + ":" + odps_columns.get(0));
                sort_column.add("string:" + odps_columns.get(1));
                sort_column.add("bigint:" + odps_columns.get(2));
            }
            return Joiner.on(",").join(sort_column);
        }

        @Override
        public void postHandler(Configuration jobConfiguration) {
        }

    }

    public static class Task extends Writer.Task {
        private Configuration dataxConf;
        HBaseBulker bulker;

        private void loadBulker() {
            bulker = new HBaseBulker();
        }

        @Override
        public void init() {
            dataxConf = getPluginJobConf();

            if (Strings.isNullOrEmpty(dataxConf.getString(Key.PREFIX_FIXED)) && Strings.isNullOrEmpty(dataxConf.getString(Key.PREFIX_DYNAMIC))) {
                throw DataXException.asDataXException(BulkWriterError.RUNTIME,
                        "DATAX FATAL! 没有生成有效的配置(缺少fixedcolumn or dynamiccolumn),请联系askdatax");
            }

            loadBulker();

            try {
                RetryUtil.executeWithRetry(new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        return bulker.init(dataxConf);
                    }
                }, 10, 1, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(BulkWriterError.IO, "请联系hbase同学检查hdfs集群", e);
            }
        }

        @Override
        public void destroy() {
            bulker.finish();
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            bulker.startWrite(new HBaseLineReceiver(lineReceiver), super.getTaskPluginCollector());
        }
    }
}
