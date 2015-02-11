package com.alibaba.datax.plugin.writer.adswriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.alibaba.datax.plugin.writer.adswriter.util.Key;
import com.alibaba.datax.plugin.writer.odpswriter.OdpsWriter;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;

import java.util.List;


//TODO writeProxy
public class AdsWriter extends Writer {


    public static class Job extends Writer.Job {

        private OdpsWriter.Job odpsWriterJobProxy = new OdpsWriter.Job();
        private Configuration originalConfig = null;
        private AdsHelper adsHelper;
        private final int ODPSOVERTIME = 10000;
        private String odpsTableName;

        @Override
        public void init() {

            this.originalConfig = super.getPluginJobConf();
            AdsUtil.checkNecessaryConfig(this.originalConfig);
            this.adsHelper = AdsUtil.createAdsHelp(this.originalConfig);


            //Get endpoint,accessId,accessKey,project等参数,创建ODPSConnection实例
            String endPoint = this.originalConfig.getString(Key.ODPS_SERVER);
            String accessId = this.originalConfig.getString(Key.ACCESS_ID);
            String accessKey = this.originalConfig.getString(Key.ACCESS_KEY);
            String project = this.originalConfig.getString(Key.PROJECT);
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
//                //创建odps表
                Instance instance = SQLTask.run(odps,project,sql,null,null);
                String id = instance.getId();
                boolean terminated = false;
                    int time = 0;
                    while(!terminated && time < ODPSOVERTIME)
                    {
                        Thread.sleep(1000);
                        terminated = instance.isTerminated();
                        time += 1000;
                }
            } catch (AdsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.TABLE_TRUNCATE_ERROR,e);
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

            //倒数据到odps表中
            this.odpsWriterJobProxy.prepare();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return this.odpsWriterJobProxy.split(mandatoryNumber);
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            String table = this.originalConfig.getString(Key.ADS_TABLE);
            String project = this.originalConfig.getString(Key.PROJECT);
            String partition = this.originalConfig.getString(Key.PARTITION);
            String sourcePath = AdsUtil.generateSourcePath(project,this.odpsTableName);
            boolean overwrite = true;
            try {
                String id = adsHelper.loadData(table,partition,sourcePath,overwrite);
                boolean terminated = false;
                int time = 0;
                while(!terminated)
                {
                    Thread.sleep(120000);
                    terminated = adsHelper.checkLoadDataJobStatus(id);
                }
            } catch (AdsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ADS_LOAD_DATA_FAILED,e);
            } catch (InterruptedException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }
            this.odpsWriterJobProxy.post();

        }

        @Override
        public void destroy() {
            this.odpsWriterJobProxy.destroy();
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
