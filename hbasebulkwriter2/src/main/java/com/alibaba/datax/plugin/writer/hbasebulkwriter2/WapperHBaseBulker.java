package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@SuppressWarnings("Duplicates")
public class WapperHBaseBulker {

    private static final Logger LOG = LoggerFactory.getLogger(WapperHBaseBulker.class);
    private org.apache.hadoop.conf.Configuration hbaseConf;
    private String table;
    private HTable htable;
    /**
     * HDFS path where to put temperate HFiles
     */
    private String hdfsOutputDir;

    static {
        HBaseHelper.loadNativeLibrary();
    }

    public WapperHBaseBulker() {
    }

    public void init(String table, String configurationStr, String hdfsOutputDir) throws IOException {
        LOG.info(String.format("WapperHBaseBulker hbase init[path=%s,table=%s] starting... ", hdfsOutputDir, table));
        this.table = table;
        this.hdfsOutputDir = hdfsOutputDir;
        this.hbaseConf = HBaseHelper.getConfiguration(null, null, configurationStr, null);
        HBaseHelper.checkConf(this.hbaseConf);
        HBaseHelper.checkHdfsVersion(this.hbaseConf);
        HBaseHelper.checkNativeLzoLibrary(this.hbaseConf);

        try {
            htable = new HTable(this.hbaseConf, table);
        } catch (IOException e) {
            throw DataXException.asDataXException(BulkWriterError.HBASE_ERROR, "Create HTable Failed.", e);
        }

        LOG.info(String.format("WapperHBaseBulker hbase init[path=%s,table=%s] finish", hdfsOutputDir, table));
    }

    public int doBulk() {

        LOG.info(String.format("WapperHBaseBulker hbase doBulkLoad[path=%s,table=%s] starting... ", hdfsOutputDir, table));
        try {
            String uri = hbaseConf.get("fs.default.name");

            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConf);
            /**
             * {@link LoadIncrementalHFiles#doBulkLoad(Path, HTable)} must pass a Path
             * param with uri, because the bug @see
             * <ahref="https://issues.apache.org/jira/browse/HBASE-9537"
             * >HBASE-9537</a>
             */
            loader.doBulkLoad(new Path(uri + "/" + hdfsOutputDir), htable);
            clearDir();
        } catch (Exception e) {
            LOG.error("BulkLoad error.", e);
            throw DataXException.asDataXException(BulkWriterError.HBASE_ERROR, e);
        }
        LOG.info(String.format("WapperHBaseBulker hbase doBulkLoad[path=%s,table=%s] finish ", hdfsOutputDir, table));
        return 0;
    }

    public void clearDir() {
        LOG.info(String.format("WapperHBaseBulker hbase clearDir[path=%s,table=%s] start... ", hdfsOutputDir, table));
        HBaseHelper.clearTmpOutputDir(hbaseConf, hdfsOutputDir);
        LOG.info(String.format("WapperHBaseBulker hbase clearDir[path=%s,table=%s] finish ", hdfsOutputDir, table));
    }
}
