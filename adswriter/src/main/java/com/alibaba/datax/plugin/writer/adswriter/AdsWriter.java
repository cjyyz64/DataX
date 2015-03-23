package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import com.alibaba.datax.plugin.writer.adswriter.util.PropertyLoader;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriter;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class AdsWriter extends Writer {


    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Writer.Job.class);

        private OdpsWriter.Job odpsWriterJobProxy = new OdpsWriter.Job();
        private Configuration originalConfig = null;
        private Configuration readerConfig = null;
        private AdsHelper adsHelper;
        private final int ODPSOVERTIME = 120000;
        private String odpsTableName;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            AdsUtil.checkNecessaryConfig(this.originalConfig);
            this.adsHelper = AdsUtil.createAdsHelp(this.originalConfig);
            if(this.adsHelper == null) {
                throw DataXException.asDataXException(AdsWriterErrorCode.Create_ADSHelper_FAILED, "");
            }
            /*检查ReaderPlugin是否为MySQLReader,执行special Pattern*/
            String readerPluginName = super.getReaderPluginName();
            if (readerPluginName.equals(Key.ODPSREADER)){
                this.readerConfig = super.getReaderConf();
                String odpsTableName = this.readerConfig.getString(Key.ODPSTABLENAME);
                List<String> userConfiguredPartitions = this.readerConfig.getList(
                        Key.PARTITION, String.class);
                if(userConfiguredPartitions.size() > 1)
                    throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_PARTITION_FAILED,"");
                if(userConfiguredPartitions.size() == 0){
                    loadAdsData(odpsTableName,null);
                }else{
                    loadAdsData(odpsTableName,userConfiguredPartitions.get(0));
                }
                System.exit(0);
            }

            //Get endpoint,accessId,accessKey,project等参数,创建ODPSConnection实例
            String endPoint = PropertyLoader.getString(Key.CONFIG_ODPS_SERVER);
            String accessId = PropertyLoader.getString(Key.CONFIG_ACCESS_ID);
            String accessKey = PropertyLoader.getString(Key.CONFIG_ACCESS_KEY);
            String project = PropertyLoader.getString(Key.CONFIG_PROJECT);

            TableMeta tableMeta;
            Account odpsAccount = new AliyunAccount(accessId,accessKey);
            Odps odps = new Odps(odpsAccount);
            odps.setEndpoint(endPoint);

            try {
                String adsTable = this.originalConfig.getString(Key.ADS_TABLE);
                int lifeCycle = this.originalConfig.getInt(Key.Life_CYCLE);
                TableInfo tableInfo = adsHelper.getTableInfo(adsTable);
                tableMeta = TableMetaHelper.createTempODPSTable(tableInfo,lifeCycle);
                this.odpsTableName = tableMeta.getTableName();
                String sql = tableMeta.toDDL();
                LOG.info("正在创建ODPS临时表： "+sql);
//                //创建odps表
                Instance instance = SQLTask.run(odps,project,sql,null,null);
                boolean terminated = false;
                    int time = 0;
                    while(!terminated && time < ODPSOVERTIME)
                    {
                        Thread.sleep(1000);
                        terminated = instance.isTerminated();
                        time += 1000;
                }
            } catch (AdsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }catch (OdpsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            } catch (InterruptedException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }

            Configuration newConf = AdsUtil.generateConf(this.originalConfig,this.odpsTableName,tableMeta);
            odpsWriterJobProxy.setPluginJobConf(newConf);
            odpsWriterJobProxy.init();
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {

            //导数据到odps表中
            this.odpsWriterJobProxy.prepare();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.odpsWriterJobProxy.split(mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            try{
                loadAdsData(this.odpsTableName,null);
            }catch(DataXException e){
                throw e;
            }
            this.odpsWriterJobProxy.post();

        }

        @Override
        public void destroy() {
            this.odpsWriterJobProxy.destroy();
        }

        private boolean loadAdsData(String odpsTableName, String odpsPartition){

            String table = this.originalConfig.getString(Key.ADS_TABLE);
            String project = PropertyLoader.getString(Key.CONFIG_PROJECT);
            String partition = this.originalConfig.getString(Key.PARTITION);
            String sourcePath = AdsUtil.generateSourcePath(project,odpsTableName,odpsPartition);
            boolean overwrite = this.originalConfig.getBool(Key.OVER_WRITER);
            try {
                String id = adsHelper.loadData(table,partition,sourcePath,overwrite);
                boolean terminated = false;
                int time = 0;
                while(!terminated)
                {
                    Thread.sleep(120000);
                    terminated = adsHelper.checkLoadDataJobStatus(id);
                    time += 2;
                    LOG.info("ADS 正在导数据中，整个过程需要20分钟以上，请耐心等待,目前已执行 "+ time+" 分钟");
                }
                LOG.info("ADS 导数据已成功");
                return terminated;
            } catch (AdsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ADS_LOAD_DATA_FAILED,e);
            } catch (InterruptedException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }
        }
    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private OdpsWriter.Task odpsWriterTaskProxy = new OdpsWriter.Task();

        @Override
        public void init() {
            writerSliceConfig = super.getPluginJobConf();
            odpsWriterTaskProxy.setPluginJobConf(writerSliceConfig);
            odpsWriterTaskProxy.init();
        }

        @Override
        public void prepare() {
            odpsWriterTaskProxy.prepare();
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
            odpsWriterTaskProxy.setTaskPluginCollector(super.getTaskPluginCollector());
            odpsWriterTaskProxy.startWrite(recordReceiver);
        }

        @Override
        public void post() {
            odpsWriterTaskProxy.post();
        }

        @Override
        public void destroy() {
            odpsWriterTaskProxy.destroy();
        }
    }

}
