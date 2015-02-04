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

        private OdpsWriter.Job odpsWriterProxy = new OdpsWriter.Job();
        private Configuration originalConfig = null;
        private AdsHelper adsHelper;
        private final int ODPSOVERTIME = 10000;

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
            Account odpsAccount = new AliyunAccount(accessId,accessKey);
            Odps odps = new Odps(odpsAccount);
            odps.setEndpoint(endPoint);

            try {
                String adsTable = originalConfig.getString(Key.TABLE);
                int lifeCycle = originalConfig.getInt(Key.Life_CYCLE);
                TableInfo tableInfo = adsHelper.getTableInfo(adsTable);
                TableMeta tableMeta = TableMetaHelper.createTempODPSTable(tableInfo,lifeCycle);
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

            Configuration newConf = AdsUtil.generateConf(this.originalConfig);
            super.setPluginConf(newConf);
        }

        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            //倒数据到odps表中

            this.odpsWriterProxy.init();
            this.odpsWriterProxy.prepare();
            this.odpsWriterProxy.split(3);
            this.odpsWriterProxy.post();
            this.odpsWriterProxy.destroy();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return null;
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {
            String table = null;
            String sourcePath = null;
            boolean overwrite = false;
            try {
                String id = adsHelper.loadData(table,sourcePath,overwrite);
                boolean terminated = false;
                int time = 0;
                while(!terminated && time < ODPSOVERTIME)
                {
                    Thread.sleep(1000);
                    terminated = adsHelper.checkLoadDataJobStatus(id);
                    time += 1000;
                }
            } catch (AdsException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ADS_LOAD_DATA_FAILED,e);
            } catch (InterruptedException e) {
                throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
            }

        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;

        @Override
        public void init() {
        }

        @Override
        public void prepare() {
        }

        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver recordReceiver) {
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

}
