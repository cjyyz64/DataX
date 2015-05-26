package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf.DynamicColumnConf;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf.FixColumnConf;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.conf.HBaseJobParameterConf;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HBaseBulkWriter2 extends Writer {

    public static final String DATAX_HOME = System.getProperty("datax.home", "/home/admin/datax3");
    public static final String PLUGIN_HOME = DATAX_HOME + "/plugin/writer/hbasebulkwriter2";
    public static final String ODPS_SORT_SCRIPT = PLUGIN_HOME + "/datax_odps_hbase_sort.py";


    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(HBaseBulkWriter2.class);


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
            loadBulker();
            try {
                bulker.init(dataxConf);
            } catch (IOException e) {
                throw DataXException.asDataXException(BulkWriterError.IO, e);
            }
        }


        @Override
        public void prepare() {
            bulker.prepare();
        }

        @Override
        public void destroy() {
            try {
                bulker.finish();
            } catch (Exception e) {
                throw DataXException.asDataXException(BulkWriterError.IO, e);
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

            Configuration readerOriginPluginConf = jobConfiguration.getConfiguration(Key.READER_PARAMETER);
            Configuration writerOriginPluginConf = jobConfiguration.getConfiguration(Key.WRITER_PARAMETER + "." + Key.PARAMETER_TYPE_ORIGIN);
            //"job.content[0].reader.parameter.column"
            List<String> odps_column = readerOriginPluginConf.getList(Key.KEY_COLUMN, String.class);

            if (1 == odps_column.size()
                    && "*".equals(odps_column.get(0))) {
                LOG.error("odps_column不能配置为*");
                throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "odps_column不能配置为*");

            }

            String hbase_rowkey = writerOriginPluginConf.getString(Key.KEY_HBASE_ROWKEY);
            String rowkey_type = writerOriginPluginConf.getString(Key.KEY_ROWKEY_TYPE);

            if (Strings.isNullOrEmpty(hbase_rowkey) && Strings.isNullOrEmpty(rowkey_type)) {
                LOG.error("hbase_rowkey和rowkey_type不能同时为空");
                throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING, "hbase_rowkey和rowkey_type不能同时为空");
            }

            if (!Strings.isNullOrEmpty(hbase_rowkey) && !Strings.isNullOrEmpty(rowkey_type)) {
                LOG.error("hbase_rowkey和rowkey_type不能同时配置");
                throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "hbase_rowkey和rowkey_type不能同时配置");
            }

            String sort_column = getSortColumn(odps_column, hbase_rowkey, rowkey_type);

            String odpsProject = readerOriginPluginConf.getString(Key.KEY_PROJECT);
            String odpsTable = readerOriginPluginConf.getString(Key.KEY_TABLE);
            List<String> partition = readerOriginPluginConf.getList(Key.KEY_PARTITION, String.class);

            String dstTable = writerOriginPluginConf.getString(Key.KEY_HBASE_TABLE);
            String hbaseXml = writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIG);

            //
            String clusterId = writerOriginPluginConf.getString(Key.KEY_CLUSTERID);

            String suffix = UUID.randomUUID().toString().replace("-", "a");

            String bucketNum = writerOriginPluginConf.getString(Key.KEY_BUCKETNUM);
            String dynamicQualifier = "false";
            if (Strings.isNullOrEmpty(hbase_rowkey) && !Strings.isNullOrEmpty(rowkey_type)) {
                dynamicQualifier = "true";
            }
            String accessId = readerOriginPluginConf.getString(Key.KEY_ACCESSID);
            String accessKey = readerOriginPluginConf.getString(Key.KEY_ACCESSKEY);

            List<String> cmdList = new ArrayList<String>();
            cmdList.add("python");
            cmdList.add(ODPS_SORT_SCRIPT);
            cmdList.add("--project");
            cmdList.add(odpsProject);
            cmdList.add("--src_table");
            cmdList.add(odpsTable);
            cmdList.add("--sort_column");
            cmdList.add(sort_column);

            String parts = getPartitions(partition);
            if (!Strings.isNullOrEmpty(parts)) {
                cmdList.add("--partition");
                cmdList.add(parts);
            }
            cmdList.add("--dst_table");
            cmdList.add(dstTable);
            cmdList.add("--hbase_config");
            cmdList.add(hbaseXml);

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

            ProcessBuilder builder = new ProcessBuilder(cmdList);

            LOG.info("run sort cmd: {} ", cmdList.toString());
            builder.redirectErrorStream(true);
            try {
                Process p = builder.start();

                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    LOG.info("ODPS_SORT_SCRIPT => " + line);
                }
                int resCode = p.waitFor();

                if (resCode != 0) {
                    LOG.error("{} run failed, rescode={}", ODPS_SORT_SCRIPT, resCode);
                    throw DataXException.asDataXException(BulkWriterError.RUNTIME, ODPS_SORT_SCRIPT + "run failed for rescode=" + resCode);
                }

            } catch (IOException e) {
                LOG.error("{} run Exception {}", ODPS_SORT_SCRIPT, e.getMessage());
                throw DataXException.asDataXException(BulkWriterError.RUNTIME, ODPS_SORT_SCRIPT + "run has Exception ", e);
            } catch (InterruptedException e) {
                LOG.error("{} run Exception {}", ODPS_SORT_SCRIPT, e.getMessage());
                throw DataXException.asDataXException(BulkWriterError.RUNTIME, ODPS_SORT_SCRIPT + "run has Exception ", e);
            }

            String real_odps_table = String.format("t_datax_odps2hbase_table_%s%s", odpsTable, suffix);

            //change the origin configuration

            //for reader:
            jobConfiguration.set("job.content[0].reader.parameter.table", real_odps_table);
            jobConfiguration.set("job.content[0].reader.parameter.partition", "datax_pt=*");

            //for writer:


            HBaseJobParameterConf hBaseJobParameterConf;
            String mode;
            if ("true".equals(dynamicQualifier)) {
                mode = "dynamiccolumn";
                hBaseJobParameterConf = getDynamicColumnConf(writerOriginPluginConf);
            } else {
                mode = "fixedcolumn";
                hBaseJobParameterConf = getFixColumnConf(writerOriginPluginConf);
            }


            jobConfiguration.set("job.content[0].writer.parameter." + mode, JSON.toJSONString(hBaseJobParameterConf));

            LOG.info("final wirter job.json: {}", jobConfiguration.getString("job.content[0].writer.parameter." + mode));

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

        private FixColumnConf getFixColumnConf(Configuration writerOriginPluginConf) {
            String hbase_column = writerOriginPluginConf.getString(Key.KEY_HBASE_COLUMN);

            String[] hbaseColumns = hbase_column.split(",");
            for (String hColumn : hbaseColumns) {
                String[] ele = hColumn.split("\\|");
                FixColumnConf.HbaseColumn hbaseColumn = new FixColumnConf.HbaseColumn();
                hbaseColumn.index = ele[0];
                hbaseColumn.htype = ele[1];
                hbaseColumn.hname = ele[2];
                fixColumnConf.getHbase_column().add(hbaseColumn);
            }

            fixColumnConf.hbase_table = writerOriginPluginConf.getString(Key.KEY_HBASE_TABLE);
            fixColumnConf.hbase_output = writerOriginPluginConf.getString(Key.KEY_HBASE_OUTPUT);
            fixColumnConf.hbase_config = writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIG);
            fixColumnConf.hdfs_config = writerOriginPluginConf.getString(Key.KEY_HDFS_CONFIG);
            fixColumnConf.optional = (Map<String, String>) writerOriginPluginConf.get(Key.KEY_OPTIONAL, Map.class);


            return fixColumnConf;
        }

        private DynamicColumnConf getDynamicColumnConf(Configuration writerOriginPluginConf) {
            String hbase_column = writerOriginPluginConf.getString(Key.KEY_HBASE_COLUMN);
            String[] hbaseColumns = hbase_column.split(",");
            for (String hColumn : hbaseColumns) {
                String[] ele = hColumn.split("\\|");
                DynamicColumnConf.HBaseRule hBaseRule = new DynamicColumnConf.HBaseRule();
                hBaseRule.pattern = ele[1];
                hBaseRule.htype = ele[0];
                dynamicColumnConf.getHbase_column().getRules().add(hBaseRule);
            }

            dynamicColumnConf.hbase_table = writerOriginPluginConf.getString(Key.KEY_HBASE_TABLE);
            dynamicColumnConf.hbase_output = writerOriginPluginConf.getString(Key.KEY_HBASE_OUTPUT);
            dynamicColumnConf.hbase_config = writerOriginPluginConf.getString(Key.KEY_HBASE_CONFIG);
            dynamicColumnConf.hdfs_config = writerOriginPluginConf.getString(Key.KEY_HDFS_CONFIG);
            dynamicColumnConf.rowkey_type = writerOriginPluginConf.getString(Key.KEY_ROWKEY_TYPE);

            return dynamicColumnConf;
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

                        fixColumnConf.getHbase_rowkey().add(hbaseRowkey);

                    } else {
                        if (elem.length != 3) {
                            LOG.error("hbase rowkey的index为-1,则格式必须为-1|type|fixed");
                            throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "hbase rowkey的index为-1,则格式必须为-1|type|fixed");
                        }
                        String fixed = elem[2];
                        if (typ.equals("string")) {
                            fixed = "'" + fixed + "'";
                        }
                        sort_column.add(typ + ":" + fixed);

                        //for fixColumnConf
                        FixColumnConf.RowkeyColumn hbaseRowkey = new FixColumnConf.RowkeyColumn();
                        hbaseRowkey.index = String.valueOf(index);
                        hbaseRowkey.htype = typ;
                        hbaseRowkey.constant = fixed;

                        fixColumnConf.getHbase_rowkey().add(hbaseRowkey);
                    }
                }
            } else {
                //for dynamic column
                if (odps_columns.size() != 4 || Strings.isNullOrEmpty(rowkey_type)) {
                    LOG.error("动态列必须为4元组:rowkey,cf_qual,ts,val,且rowkey_type不能为空");
                    throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL, "动态列必须为4元组:rowkey,cf_qual,ts,val,且rowkey_type不能为空");
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
            loadBulker();
            try {
                bulker.init(dataxConf);
            } catch (IOException e) {
                throw DataXException.asDataXException(BulkWriterError.IO, e);
            }
        }

        @Override
        public void destroy() {
            bulker.finish();
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            bulker.startWrite(new HBaseLineReceiver(lineReceiver),super.getTaskPluginCollector());
        }
    }
}
