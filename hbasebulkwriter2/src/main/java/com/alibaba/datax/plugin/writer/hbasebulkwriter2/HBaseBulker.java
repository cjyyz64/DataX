package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.column.DynamicHBaseColumn;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.column.FixedHBaseColumn;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.column.HBaseColumn;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HBaseBulker {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseBulker.class);
    private org.apache.hadoop.conf.Configuration hbaseConf;
    private String hdfsConfPath;
    private String hbaseConfPath;
    private String configurationStr;
    // diamond cluster id, use diamond to save hbase-site.xml and hdfs-site.xml, diamond is a service
    @Deprecated
    private String diamondClusterId;
    private boolean isDynamicQualifier;
    @Deprecated
    private boolean isTruncateTable;
    private HBaseColumn.HBaseDataType rowkeyType;
    private String table;
    private int bucketNum;
    private int timeCol;
    private long startTs;
    private String nullMode;
    private HTable htable;
    @Deprecated
    private String encoding;
    /**
     * HDFS path where to put temperate HFiles
     */
    private String hdfsOutputDir;
    private List<? extends HBaseColumn> columnList;
    private List<FixedHBaseColumn> rowkeyList;
    private HFileWriter hfileWriter;

    static {
        HBaseHelper.loadNativeLibrary();
    }

    public HBaseBulker() {
    }

    // For test
    public int init(String table, org.apache.hadoop.conf.Configuration hbaseConf,
                    List<FixedHBaseColumn> rowkeyList,
                    List<FixedHBaseColumn> columnList)
            throws IOException {
        this.table = table;
        this.htable = new HTable(hbaseConf, table);
        this.hbaseConf = hbaseConf;
        this.rowkeyList = rowkeyList;
        this.columnList = columnList;
        return 0;
    }

    public int init(Configuration dataxConf) throws IOException {

        if (dataxConf.get(Key.PREFIX_FIXED) != null) {
            LOG.info("bulkwriter in fixed column mode");
            loadFixedColumnConfig(dataxConf);
        } else if (dataxConf.get(Key.PREFIX_DYNAMIC) != null) {
            LOG.info("bulkwriter in dynamic column mode");
            loadDynamicColumnConfig(dataxConf);
        } else {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME,
                    "DATAX FATAL! 没有生成有效的配置(缺少fixedcolumn or dynamiccolumn)");
        }
        if (this.hbaseConf == null) {
            this.hbaseConf = HBaseHelper.getConfiguration(hdfsConfPath,
                    hbaseConfPath, configurationStr, diamondClusterId);
        }
        HBaseHelper.checkConf(this.hbaseConf);
        HBaseHelper.checkHdfsVersion(this.hbaseConf);
        HBaseHelper.checkNativeLzoLibrary(this.hbaseConf);

        try {
            htable = new HTable(this.hbaseConf, table);
        } catch (IOException e) {
            throw DataXException.asDataXException(BulkWriterError.HBASE_ERROR,
                    "Create HTable Failed.", e);
        }

        return 0;
    }

    public void loadDynamicColumnConfig(Configuration conf) {
        this.table = conf.getString(Key.PREFIX_DYNAMIC + "." + Key.KEY_HBASE_TABLE);
        if (this.table == null) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "Missing config hbaseTable. 请联系askdatax ");
        }
        String rowkeyTypeStr = conf
                .getString(Key.PREFIX_DYNAMIC + "." + Key.KEY_ROWKEY_TYPE);
        if (rowkeyTypeStr == null) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING, "Missing conif rowkeyype");
        }
        this.rowkeyType = HBaseColumn.HBaseDataType.parseStr(rowkeyTypeStr);
        if (this.rowkeyType == null) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "Missing config rowkeyType. 请联系askdatax");
        }
        this.columnList = DynamicHBaseColumn.parseColumnStr(conf
                .getConfiguration(Key.PREFIX_DYNAMIC + "." + Key.KEY_HBASE_COLUMN));

        this.configurationStr = conf.getString(Key.PREFIX_DYNAMIC + "." + Key.KEY_HBASE_CONFIGURATION);

        this.hbaseConfPath = conf.getString(Key.PREFIX_DYNAMIC + "." + Key.KEY_HBASE_CONFIG);
        if (Strings.isNullOrEmpty(this.hbaseConfPath) && Strings.isNullOrEmpty(this.configurationStr)) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "DATAX Fatal! 缺少hbaseConfig和hbaseClusterName，2者并选其一. 请联系askdatax");
        }
        this.hdfsConfPath = conf.getString(Key.PREFIX_DYNAMIC + "." + Key.KEY_HDFS_CONFIG);
        if (Strings.isNullOrEmpty(this.hdfsConfPath) && Strings.isNullOrEmpty(this.configurationStr)) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "DATAX Fatal! 缺少hdfsConfPath和hbaseClusterName，2者并选其一. 请联系askdatax");
        }

        this.diamondClusterId = conf.getString(Key.PREFIX_DYNAMIC + "." + Key.KEY_CLUSTERID);
        this.isTruncateTable = conf.getBool(
                Key.PREFIX_DYNAMIC + "." + Key.KEY_TRUNCATE, false);
        this.encoding = conf.getString(Key.PREFIX_DYNAMIC + "." + Key.KEY_ENCODING, "utf-8");
        this.hdfsOutputDir = conf.getString(Key.PREFIX_DYNAMIC + "." + Key.KEY_HBASE_OUTPUT);
        if (this.hdfsOutputDir == null) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "DATAX Fatal! 缺少 config hbase_output.请联系askdatax ");
        }
        this.isDynamicQualifier = true;
    }

    public void loadFixedColumnConfig(Configuration conf) {
        this.table = conf.getString(Key.PREFIX_FIXED + "." + Key.KEY_HBASE_TABLE);
        if (this.table == null) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "Missing config hbaseTable. 请联系askdatax");
        }
        this.rowkeyList = FixedHBaseColumn.parseRowkeySchema(conf
                .getListConfiguration(Key.PREFIX_FIXED + "." + Key.KEY_HBASE_ROWKEY));
        if (this.rowkeyList == null) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "Missing config hbaseRowkey. 请联系askdatax");
        }
        this.columnList = FixedHBaseColumn.parseColumnSchema(conf
                .getListConfiguration(Key.PREFIX_FIXED + "." + Key.KEY_HBASE_COLUMN));


        this.configurationStr = conf.getString(Key.PREFIX_FIXED + "." + Key.KEY_HBASE_CONFIGURATION);

        this.hbaseConfPath = conf.getString(Key.PREFIX_FIXED + "." + Key.KEY_HBASE_CONFIG);
        if (Strings.isNullOrEmpty(this.hbaseConfPath) && Strings.isNullOrEmpty(this.configurationStr)) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "DATAX Fatal! 缺少hbaseConfig和configuration，2者并选其一. 请联系askdatax");
        }
        this.hdfsConfPath = conf.getString(Key.PREFIX_FIXED + "." + Key.KEY_HDFS_CONFIG);
        if (Strings.isNullOrEmpty(this.hdfsConfPath) && Strings.isNullOrEmpty(this.configurationStr)) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_MISSING,
                    "DATAX Fatal! 缺少hdfsConfPath和configuration，2者并选其一. 请联系askdatax");
        }

        this.diamondClusterId = conf.getString(Key.PREFIX_FIXED + "." + Key.KEY_CLUSTERID);
        this.nullMode = conf.getString(Key.PREFIX_FIXED + "." + Key.KEY_NULL_MODE,
                "EMPTY_BYTES");
        this.bucketNum = conf.getInt(Key.PREFIX_FIXED + "." + Key.KEY_BUCKET_NUM, -1);
        this.startTs = conf.getLong(Key.PREFIX_FIXED + "." + Key.KEY_START_TS, -1);
        this.timeCol = conf.getInt(Key.PREFIX_FIXED + "." + Key.KEY_TIME_COL, -1);
        if (timeCol != -1 && startTs != -1) {
            throw DataXException.asDataXException(BulkWriterError.CONFIG_ILLEGAL,
                    "can not set timeCol and startTs at the same time.");
        }
        this.isTruncateTable = conf.getBool(Key.PREFIX_FIXED + "." + Key.KEY_TRUNCATE,
                false);
        this.encoding = conf.getString(Key.PREFIX_FIXED + "." + Key.KEY_ENCODING, "utf-8");
        this.hdfsOutputDir = conf.getString(Key.PREFIX_FIXED + "." + Key.KEY_HBASE_OUTPUT);
        if (Strings.isNullOrEmpty(this.hdfsOutputDir)) {
            throw DataXException.asDataXException(BulkWriterError.RUNTIME,
                    "DATAX FATAL! 没有生成有效的配置(缺少hbaseOutput),请联系askdatax");
        }

        this.isDynamicQualifier = false;
    }

    public int prepare() {

        HBaseHelper.checkTmpOutputDir(hbaseConf, hdfsOutputDir, 10);
        HBaseColumn.checkColumnList(htable, columnList, 10);
        return 0;
    }

    @SuppressWarnings("unchecked")
    public int startWrite(HBaseLineReceiver receiver, TaskPluginCollector taskPluginCollector) {
        LOG.info("================ HBaseBulkWriter Phase 2 odps=>hdfs starting... ================ ");
        try {
            hfileWriter = HBaseHelper.createHFileWriter(htable, hbaseConf, hdfsOutputDir);
            if (!isDynamicQualifier) {
                WritePolicer.fixed(hfileWriter, receiver, bucketNum, rowkeyList,
                        (List<FixedHBaseColumn>) columnList, encoding, timeCol, startTs,
                        nullMode, taskPluginCollector);
            } else {
                WritePolicer.dynamic(hfileWriter, receiver, rowkeyType,
                        (List<DynamicHBaseColumn>) columnList, taskPluginCollector);
            }
            LOG.info("================ HBaseBulkWriter Phase 2 odps=>hdfs finish... ================ ");
            return 0;
        } catch (Throwable e) {
            clearDir();
            throw DataXException.asDataXException(BulkWriterError.RUNTIME, e);
        }

    }

    public int finish() {
        try {
            if (hfileWriter != null) {
                hfileWriter.close();
                hfileWriter = null;
            }
        } catch (IOException e) {
            LOG.error("Close writer failed.", e);
        } catch (InterruptedException e) {
            LOG.error("Close writer failed.", e);
        }
        return 0;
    }

    public int post() {

        LOG.info("================ HBaseBulkWriter Phase 3 hbase doBulkLoad starting... ================ ");
        try {
            String uri = hbaseConf.get("fs.default.name");
            if (isTruncateTable) {
                HBaseAdmin hadmin = new HBaseAdmin(hbaseConf);
                if (hadmin.isTableEnabled(table)) {
                    hadmin.disableTable(table);
                }
                hadmin.truncateTable(table);
                hadmin.close();
            }

            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
            /**
             * {@link LoadIncrementalHFiles#doBulkLoad(Path, HTable)} must pass a Path
             * param with uri, because the bug @see
             * <ahref="https://issues.apache.org/jira/browse/HBASE-9537"
             * >HBASE-9537</a>
             */
            loader.doBulkLoad(new Path(uri + "/" + hdfsOutputDir), htable);
        } catch (Exception e) {
            LOG.error("BulkLoad error.", e);
            throw DataXException.asDataXException(BulkWriterError.HBASE_ERROR, e);
        } finally {
            clearDir();
        }
        LOG.info("================ HBaseBulkWriter Phase 3 hbase doBulkLoad finish... ================ ");
        return 0;
    }

    public void clearDir() {
        HBaseHelper.clearTmpOutputDir(hbaseConf, hdfsOutputDir);
    }
}
