package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.adsload.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.adsload.TableMetaHelper;
import com.alibaba.datax.plugin.writer.adswriter.adsload.TransferProjectConf;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.alibaba.datax.plugin.writer.adswriter.util.Constant;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriter;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.TaobaoAccount;
import com.aliyun.odps.task.SQLTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 逻辑复杂，请看文档：http://gitlab.alibaba-inc.com/datax/datax/wikis/datax-plugin-adswriter
 */
public class AdsWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.ADS;

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Writer.Job.class);
        public final static String ODPS_READER = "odpsreader";

        private OdpsWriter.Job odpsWriterJobProxy = new OdpsWriter.Job();
        private Configuration originalConfig;
        private Configuration readerConfig;

        private AdsRdbmsCommonWriter.Job commonRdbmsWriterJob;
        /**
         * 持有ads账号的ads helper
         */
        private AdsHelper adsHelper;
        /**
         * 持有odps账号的ads helper
         */
        private AdsHelper odpsAdsHelper;
        /**
         * 中转odps的配置，对应到writer配置的parameter.odps部分
         */
        private TransferProjectConf transProjConf;
        private final int ODPSOVERTIME = 120000;
        private String odpsTransTableName;

        private String writeMode;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            AdsUtil.checkNecessaryConfig(this.originalConfig);
            this.writeMode = this.originalConfig.getString(Key.WRITE_MODE);

            if(Constant.LOADMODE.equalsIgnoreCase(this.writeMode)) {
                loadModeInit();
            } else if(Constant.INSERTMODE.equalsIgnoreCase(this.writeMode) || Constant.REPLACEMODE.equalsIgnoreCase(this.writeMode)) {
                this.originalConfig = AdsUtil.adsConfToRdbmsConf(originalConfig);
                this.originalConfig.set(Key.WRITE_MODE, writeMode);
                this.commonRdbmsWriterJob = new AdsRdbmsCommonWriter.Job(DATABASE_TYPE);
                this.commonRdbmsWriterJob.init(this.originalConfig);
            } else {
                throw DataXException.asDataXException(AdsWriterErrorCode.INVALID_CONFIG_VALUE, "writeMode 必须为 'load' 或者 'insert' 或者 'replace'");
            }
        }

        private void loadModeInit() {
            this.adsHelper = AdsUtil.createAdsHelper(this.originalConfig);
            this.odpsAdsHelper = AdsUtil.createAdsHelperWithOdpsAccount(this.originalConfig);
            this.transProjConf = TransferProjectConf.create(this.originalConfig);

            /**
             * 如果是从odps导入到ads，直接load data然后System.exit()
             */
            if (super.getReaderPluginName().equals(ODPS_READER)){
                transferFromOdpsAndExit();
            }


            Account odpsAccount;
            if ("aliyun".equalsIgnoreCase(transProjConf.getAccountType())) {
                odpsAccount = new AliyunAccount(transProjConf.getAccessId(), transProjConf.getAccessKey());
            } else {
                odpsAccount = new TaobaoAccount(transProjConf.getAccessId(), transProjConf.getAccessKey());
            }

            Odps odps = new Odps(odpsAccount);
            odps.setEndpoint(transProjConf.getOdpsServer());
            odps.setDefaultProject(transProjConf.getProject());

            TableMeta tableMeta;
            try {
                String adsTable = this.originalConfig.getString(Key.ADS_TABLE);
                TableInfo tableInfo = adsHelper.getTableInfo(adsTable);
                int lifeCycle = this.originalConfig.getInt(Key.Life_CYCLE);
                tableMeta = TableMetaHelper.createTempODPSTable(tableInfo, lifeCycle);
                this.odpsTransTableName = tableMeta.getTableName();
                String sql = tableMeta.toDDL();
                LOG.info("正在创建ODPS临时表： "+sql);
                Instance instance = SQLTask.run(odps, transProjConf.getProject(), sql, null, null);
                boolean terminated = false;
                int time = 0;
                while (!terminated && time < ODPSOVERTIME) {
                    Thread.sleep(1000);
                    terminated = instance.isTerminated();
                    time += 1000;
                }
                LOG.info("正在创建ODPS临时表成功");
            } catch (AdsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED, e);
            }catch (OdpsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            } catch (InterruptedException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }

            Configuration newConf = AdsUtil.generateConf(this.originalConfig, this.odpsTransTableName,
                    tableMeta, this.transProjConf);
            odpsWriterJobProxy.setPluginJobConf(newConf);
            odpsWriterJobProxy.init();
        }

        /**
         * 当reader是odps的时候，直接call ads的load接口，完成后退出。
         * 这种情况下，用户在odps reader里头填写的参数只有部分有效。
         * 其中accessId、accessKey是忽略掉iao的。
         */
        private void transferFromOdpsAndExit() {
            this.readerConfig = super.getReaderConf();
            String odpsTableName = this.readerConfig.getString(Key.ODPSTABLENAME);
            List<String> userConfiguredPartitions = this.readerConfig.getList(Key.PARTITION, String.class);

            if (userConfiguredPartitions == null) {
                userConfiguredPartitions = Collections.emptyList();
            }

            if(userConfiguredPartitions.size() > 1)
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_PARTITION_FAILED, "");

            if(userConfiguredPartitions.size() == 0){
                loadAdsData(adsHelper, odpsTableName,null);
            }else{
                loadAdsData(adsHelper, odpsTableName,userConfiguredPartitions.get(0));
            }
            System.exit(0);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            if("load".equals(this.writeMode)) {
                //导数据到odps表中
                this.odpsWriterJobProxy.prepare();
            } else {
                //todo 目前insert模式不支持presql
                this.commonRdbmsWriterJob.prepare(this.originalConfig);
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            if("load".equals(this.writeMode)) {
                return this.odpsWriterJobProxy.split(mandatoryNumber);
            } else {
                List<Configuration> splitResult = new ArrayList<Configuration>();
                splitResult.add(this.originalConfig.clone());
                return splitResult;
            }
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            if("load".equals(this.writeMode)) {
                loadAdsData(odpsAdsHelper, this.odpsTransTableName, null);
                this.odpsWriterJobProxy.post();
            } else {
                //todo 目前insert模式不支持presql
                this.commonRdbmsWriterJob.prepare(this.originalConfig);
            }
        }

        @Override
        public void destroy() {
            if("load".equals(this.writeMode)) {
                this.odpsWriterJobProxy.destroy();
            } else {
                //do nothing
            }
        }

        private void loadAdsData(AdsHelper helper, String odpsTableName, String odpsPartition){

            String table = this.originalConfig.getString(Key.ADS_TABLE);
            String project;
            if (super.getReaderPluginName().equals(ODPS_READER)){
                project = this.readerConfig.getString(Key.PROJECT);
            }else{
                project = this.transProjConf.getProject();
            }
            String partition = this.originalConfig.getString(Key.PARTITION);
            String sourcePath = AdsUtil.generateSourcePath(project,odpsTableName,odpsPartition);
            /**
             * 因为之前检查过，所以不用担心unbox的时候NPE
             */
            boolean overwrite = this.originalConfig.getBool(Key.OVER_WRITE);
            try {
                String id = helper.loadData(table,partition,sourcePath,overwrite);
                LOG.info("ADS Load Data任务已经提交，job id: " + id);
                boolean terminated = false;
                int time = 0;
                while(!terminated)
                {
                    Thread.sleep(120000);
                    terminated = helper.checkLoadDataJobStatus(id);
                    time += 2;
                    LOG.info("ADS 正在导数据中，整个过程需要20分钟以上，请耐心等待,目前已执行 "+ time+" 分钟");
                }
                LOG.info("ADS 导数据已成功");
            } catch (AdsException e) {
                if (super.getReaderPluginName().equals(ODPS_READER)){
                    // TODO 使用云账号
                    AdsWriterErrorCode.ADS_LOAD_ODPS_FAILED.setAdsAccount(helper.getUserName());
                    throw DataXException.asDataXException(AdsWriterErrorCode.ADS_LOAD_ODPS_FAILED,e);
                }else{
                    throw DataXException.asDataXException(AdsWriterErrorCode.ADS_LOAD_TEMP_ODPS_FAILED,e);
                }
            } catch (InterruptedException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }
        }
    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private OdpsWriter.Task odpsWriterTaskProxy = new OdpsWriter.Task();

        private AdsRdbmsCommonWriter.Task commonRdbmsWriterTask;

        private String writeMode;

        @Override
        public void init() {
            writerSliceConfig = super.getPluginJobConf();
            this.writeMode = this.writerSliceConfig.getString(Key.WRITE_MODE);

            if("load".equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.setPluginJobConf(writerSliceConfig);
                odpsWriterTaskProxy.init();
            } else if("insert".equalsIgnoreCase(this.writeMode)) {
                this.writerSliceConfig = super.getPluginJobConf();
                this.commonRdbmsWriterTask = new AdsRdbmsCommonWriter.Task(DATABASE_TYPE);
                this.commonRdbmsWriterTask.init(this.writerSliceConfig);
            } else {
                throw DataXException.asDataXException(AdsWriterErrorCode.INVALID_CONFIG_VALUE, "writeMode 必须为 'load' 或者 'insert'");
            }

        }

        @Override
        public void prepare() {
            if("load".equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.prepare();
            } else {
                //todo 目前insert模式不支持presql
                this.commonRdbmsWriterTask.prepare(this.writerSliceConfig);
            }
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
            if("load".equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.setTaskPluginCollector(super.getTaskPluginCollector());
                odpsWriterTaskProxy.startWrite(recordReceiver);
            } else {
                //todo insert 模式
                this.commonRdbmsWriterTask.startWrite(recordReceiver, this.writerSliceConfig,
                        super.getTaskPluginCollector());
            }
        }

        @Override
        public void post() {
            if("load".equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.post();
            } else {
                this.commonRdbmsWriterTask.post(this.writerSliceConfig);
            }
        }

        @Override
        public void destroy() {
            if("load".equalsIgnoreCase(this.writeMode)) {
                odpsWriterTaskProxy.destroy();
            } else {
                this.commonRdbmsWriterTask.destroy(this.writerSliceConfig);
            }
        }
    }

}
