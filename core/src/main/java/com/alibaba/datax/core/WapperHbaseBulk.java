package com.alibaba.datax.core;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/6/19.
 */
public class WapperHbaseBulk {

    private static final Logger LOG = LoggerFactory.getLogger(WapperHbaseBulk.class);

    public static final String SKYNET_HBASEBULKLOAD_JOBCONFIG = "SKYNET_BULKLOAD_INFO";
    public static final String SKYNET_JOBID = "SKYNET_JOBID";

    private static WapperHbaseBulk instance = new WapperHbaseBulk();

    public static void main(String[] args) throws Exception {
        LOG.info("WapperHbaseBulk limitation: 1. 重跑数据时,父节点和本wapper节点的调度job和之前一致,因此父节点的输出无法保证是否真的重跑成功过,需要用户确保重跑逻辑。\n" + " 2. WapperHbaseBulk的父节点只能是同步中心的hbseBulkload节点。 ");
        LOG.info("WapperHbaseBulk starting ...");
        int exitCode = 0;

        try {
            Options options = new Options();
            options.addOption("list", true, "hbase job list for wapper.");
            options.addOption("jobId", true, "skynet Job Id.");

            BasicParser parser = new BasicParser();
            CommandLine cl = parser.parse(options, args);

            String wapperFatherlist = cl.getOptionValue("list");
            String skynetJobIdStr = cl.getOptionValue("jobId");

            if (!StringUtils.isNotEmpty(wapperFatherlist)) {
                wapperFatherlist = getListFromEnv();
            }

            if (!StringUtils.isNotEmpty(skynetJobIdStr)) {
                skynetJobIdStr = getSkeyNetIdFromEnv();
            }

            List<SkynetBulkloadWapperFatherJobIdVersion> jobList = getJobId(wapperFatherlist);

            if (jobList == null || jobList.size() == 0) {
                throw new WapperBulkLoadException("not any job found. jobListConfig = " + skynetJobIdStr);
            }

            Long skynetJobId = null;
            String errorMsg = "UNKNOWN";
            try {
                skynetJobId = Long.parseLong(skynetJobIdStr);
            } catch (Exception e) {
                errorMsg = "解析调度JobId异常:" + e.getMessage();
                LOG.error(errorMsg, e);
            }

            if (skynetJobId == null) {
                throw new WapperBulkLoadException(String.format("skynetJobId can't miss or %s. skynetJobIdStr=%s", errorMsg, skynetJobIdStr));
            }

            //检查调度的jobId。本次wapperBulkload的调度实例,如果和父节点的调度实例不是同一个,需要返回错误
            //重跑时的调度job是一致的,但是无法确认之前的目录是否真的是重跑的,
            for (SkynetBulkloadWapperFatherJobIdVersion skynetBulkloadWapperFatherJobIdVersion : jobList) {
                if (skynetBulkloadWapperFatherJobIdVersion.getJobId() != skynetJobId.longValue()) {
                    throw new WapperBulkLoadException(String.format("wapperBulkLoad的调度Job(%s)不匹配父节点(skynetJobid=%s,skynetTaskid=%s,dscJobId=%s,dscJobVersion=%s)", skynetJobId, skynetBulkloadWapperFatherJobIdVersion.getJobId(), skynetBulkloadWapperFatherJobIdVersion.getTaskId(), skynetBulkloadWapperFatherJobIdVersion.getFileId(), skynetBulkloadWapperFatherJobIdVersion.getFileVersion()));
                }
            }

            /**
             * 初始化PluginLoader，可以获取各种插件配置
             */

            List<String> plugins = new ArrayList<String>();
            plugins.add("hbasebulkwriter2");
            plugins.add("hbasebulkwriter2_11x");
            Configuration pluginsConfig = ConfigParser.parsePluginConfig(plugins);

            LoadUtil.bind(pluginsConfig);

            instance.doBulkLoad(jobList, skynetJobId);
        } catch (Exception e) {
            exitCode = 1;
            LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(e));
        }

        if (exitCode == 0) {
            LOG.info("WapperHbaseBulk success!");
        } else {
            LOG.error("WapperHbaseBulk failed!");
        }
        System.exit(exitCode);
    }

    private static String getSkeyNetIdFromEnv() {
        Map<String, String> envProp = System.getenv();
        return envProp.get(SKYNET_JOBID);
    }

    private static String getListFromEnv() {
        Map<String, String> envProp = System.getenv();
        return envProp.get(SKYNET_HBASEBULKLOAD_JOBCONFIG);
    }

    private static List<SkynetBulkloadWapperFatherJobIdVersion> getJobId(String list) {
        List<SkynetBulkloadWapperFatherJobIdVersion> result = new ArrayList<SkynetBulkloadWapperFatherJobIdVersion>();
        List<Object> listObject = JSON.parseObject(list, List.class);
        for (Object object : listObject) {
            result.add(JSON.parseObject(((JSONObject) object).toJSONString(), SkynetBulkloadWapperFatherJobIdVersion.class));
        }
        return result;
    }

    /**
     * @param jobList     父节点的配置参数
     * @param skynetJobId 非常重要,要来检查父节点的hbaseBulkoad是否和bulkLoadWapper是同一个调度Job。重跑,补数据。
     */
    private void doBulkLoad(List<SkynetBulkloadWapperFatherJobIdVersion> jobList, long skynetJobId) {

        List<HBaseBulkLoadWorker> workerList = getWorkerList(jobList);
        List<HBaseBulkLoadWorker> errorWorkerList = new ArrayList<HBaseBulkLoadWorker>();

        for (HBaseBulkLoadWorker hBaseBulkLoadWorker : workerList) {
            if (!hBaseBulkLoadWorker.validate()) {
                errorWorkerList.add(hBaseBulkLoadWorker);
            }
        }

        if (errorWorkerList.size() > 0) {
            throw new WapperBulkLoadException("存在不能bulkload的hbase,直接退出。全部hbase都不执行bulkLoad。请联系hbase PE检查错误集群:" + erroHbaseInfo(errorWorkerList));
        }

        Thread[] threads = new Thread[workerList.size()];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(workerList.get(i));
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                LOG.error("HBaseBulkLoadWorker执行线程异常", e);
            }
        }

        int sucess = 0;
        for (HBaseBulkLoadWorker hBaseBulkLoadWorker : workerList) {
            if (!hBaseBulkLoadWorker.isSuccess()) {
                errorWorkerList.add(hBaseBulkLoadWorker);
            } else {
                sucess++;
            }
        }

        if (errorWorkerList.size() > 0) {
            String errorMsg;
            if (sucess == 0) {
                errorMsg = "所有的集群bulkLoad失败。";
            } else {
                errorMsg = String.format("严重错误, 不一致情况出现!!! 只有部分集群bulkLoad成功(成功数=%s)。", sucess);
            }
            throw new WapperBulkLoadException(String.format("%s,请联系hbase PE检查错误集群: %s", errorMsg, erroHbaseInfo(errorWorkerList)));
        }
    }

    private <T extends HBaseAware> String erroHbaseInfo(List<T> errorCheckWorkerList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[错误集群数:" + errorCheckWorkerList.size() + "]");
        for (T worker : errorCheckWorkerList) {
            sb.append(" [ hbaseInfo=" + worker.getHbaseInfo(true) + " ]");
        }
        return sb.toString();
    }

    private List<HBaseBulkLoadWorker> getWorkerList(List<SkynetBulkloadWapperFatherJobIdVersion> jobList) {

        Configuration core = Configuration.from(new File(CoreConstant.DATAX_CONF_PATH));

        List<HBaseBulkLoadWorker> result = new ArrayList<HBaseBulkLoadWorker>();
        for (SkynetBulkloadWapperFatherJobIdVersion jobId : jobList) {

            Configuration jobConfig = getJobConfig(core, jobId.getFileId(), jobId.getFileVersion());

            //只检查hbaseBulkLoadControl,如果上游节点有正常的hbaseBulkWriter作业,直接报错。不商量。
            String hbaseControl = jobConfig.getString("data.writer.hbaseBulkLoadControl", "true");

            if (hbaseControl.toLowerCase().equals("true")) {
                throw new WapperBulkLoadException(String.format("上游hbaseBulkWriter任务开启了upload功能,因此wapperBulkLoad不能执行,dscJobId=%s,dscJobVersion=%s", jobId.getFileId(), jobId.getFileVersion()));
            }

            result.add(new HBaseBulkLoadWorker(jobId, jobConfig));
        }
        return result;
    }

    public Configuration getJobConfig(Configuration core, Long fileId, Long fileVersion) {

        //因为wapper跑在用户的机器上,弄不齐这个地址是错的(比如dataxservice的地址)。
        //因此再增加一个配置,以防不时之需。
        String dataxServiceUrl = core.getString(CoreConstant.DATAX_CORE_DATAXSERVER_ADDRESS);

        if (!StringUtils.isNotEmpty(dataxServiceUrl)) {
            throw new WapperBulkLoadException("core.json必须配置core.dataXServer");
        }
        if (dataxServiceUrl.endsWith("/")) {
            dataxServiceUrl = dataxServiceUrl.substring(0, dataxServiceUrl.length() - 1);
        }
        //这个配置目前还没有启用,因此增加默认值
        String dscUrl = core.getString(CoreConstant.DATAX_CORE_DSC_ADDRESS, "http://dsc.alibaba-inc.com/dsc-core/");
        if (dscUrl.endsWith("/")) {
            dscUrl = dscUrl.substring(0, dscUrl.length() - 1);
        }

        Configuration jobConfig = null;
        try {
            jobConfig = ConfigParser.parseJobConfig(String.format("%s/job/%s/config?jobVersion=%s", dataxServiceUrl, fileId, fileVersion));
        } catch (Exception e) {
            //try another dsc url
            LOG.error("即将重试,因为捕获到如下异常:", e);
            try {
                jobConfig = ConfigParser.parseJobConfig(String.format("%s/job/%s/config?jobVersion=%s", dscUrl, fileId, fileVersion));
            } catch (Exception ee) {
                LOG.error("重试获取job失败:", ee);
            }
        }
        //不做jobConfig的校验了。
        if (jobConfig == null) {
            throw new WapperBulkLoadException(String.format("获取不到指定作业的配置,请联系askdatax。dscJobId=%s,dscJobVersion=%s,dataxServiceUrl=%s,dscUrl=%s", fileId, fileVersion, dataxServiceUrl, dscUrl));
        }
        return jobConfig;
    }


    public static class SkynetBulkloadWapperFatherJobIdVersion {
        //dsc fileId, fileVersion
        Long fileId;
        Long fileVersion;
        //调度jobId,taskId
        Long jobId;
        Long taskId;

        public Long getFileId() {
            return fileId;
        }

        public Long getFileVersion() {
            return fileVersion;
        }

        public Long getJobId() {
            return jobId;
        }

        public Long getTaskId() {
            return taskId;
        }

        public void setFileId(Long fileId) {
            this.fileId = fileId;
        }

        public void setFileVersion(Long fileVersion) {
            this.fileVersion = fileVersion;
        }

        public void setJobId(Long jobId) {
            this.jobId = jobId;
        }

        public void setTaskId(Long taskId) {
            this.taskId = taskId;
        }
    }

    public interface HBaseAware {
        String getHbaseInfo(boolean printError);
    }


    public static class WapperBulkLoadException extends RuntimeException {
        public WapperBulkLoadException(String message) {
            super(message);
        }
    }


    public static class HBaseBulkLoadWorker implements Runnable, HBaseAware {

        //可以支持不同hbase版本的同时load
        private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
        private ClassLoader classLoader;

        private SkynetBulkloadWapperFatherJobIdVersion skynetWapper;
        //作业配置相关
        private String hbaseVersion;
        private String hbaseOutput;
        private String hbaseTable;
        private String hbaseConfigStr;

        //运行状态
        private volatile boolean isSuccess = false;
        private String errorMessage = "not run";

        /**
         * 简单处理,不再进行接口化。同时支持94.x, 1.1.x
         */
        private Object invokeInstance;
        private Method doBulk;

        public HBaseBulkLoadWorker(SkynetBulkloadWapperFatherJobIdVersion skynetWapper, Configuration jobConfig) {
            this.skynetWapper = skynetWapper;
            this.hbaseVersion = jobConfig.getString("data.writer.hbaseVersion", "094x");
            this.hbaseOutput = jobConfig.getString("data.writer.hbaseOutput");
            this.hbaseTable = jobConfig.getString("data.writer.hbaseTable");
            this.hbaseConfigStr = jobConfig.getString("data.writer.configuration");

            if (!StringUtils.isNotEmpty(hbaseOutput)) {
                throw new WapperBulkLoadException(String.format("上游hbaseBulkWriter任务缺失配置hbaseOutput,因此wapperBulkLoad不能执行,dscJobId=%s,dscJobVersion=%s", skynetWapper.getFileId(), skynetWapper.getFileVersion()));
            }

            if (hbaseOutput.endsWith("/")) {
                hbaseOutput = hbaseOutput.substring(0, hbaseOutput.length() - 1);
            }

            if (!StringUtils.isNotEmpty(hbaseTable)) {
                throw new WapperBulkLoadException(String.format("上游hbaseBulkWriter任务缺失配置hbaseTable,因此wapperBulkLoad不能执行,dscJobId=%s,dscJobVersion=%s", skynetWapper.getFileId(), skynetWapper.getFileVersion()));
            }

            if (!StringUtils.isNotEmpty(hbaseConfigStr)) {
                throw new WapperBulkLoadException(String.format("上游hbaseBulkWriter任务未能获取hbaseConfiguration,因此wapperBulkLoad不能执行,dscJobId=%s,dscJobVersion=%s", skynetWapper.getFileId(), skynetWapper.getFileVersion()));
            }

            this.classLoader = LoadUtil.getJarLoader(PluginType.WRITER, getPluginName());
            LOG.info(String.format("HBaseBulkLoadWorker contruct success: %s, hbaseVersion=%s, pluginName=%s" ,getHbaseInfo(false), hbaseVersion,getPluginName()));
        }

        private String getPluginName() {
            if (hbaseVersion.equalsIgnoreCase("094x")) {
                return "hbasebulkwriter2";
            } else if (hbaseVersion.equalsIgnoreCase("11x")) {
                return "hbasebulkwriter2_11x";
            } else {
                throw new WapperBulkLoadException(String.format("错误的上游hbaseBulkWriter任务的hbase版本(%s),因此wapperBulkLoad不能执行,dscJobId=%s,dscJobVersion=%s", hbaseVersion, skynetWapper.getFileId(), skynetWapper.getFileVersion()));
            }

        }

        private String getHdfsOutPut() {
            return hbaseOutput + "/dscBulkloadWapper/" + this.skynetWapper.getJobId() + "/" + this.skynetWapper.getTaskId() + "/" + hbaseTable;
        }

        private String getClassName() {
            if (hbaseVersion.equalsIgnoreCase("094x")) {
                return "com.alibaba.datax.plugin.writer.hbasebulkwriter2.WapperHBaseBulker";
            } else if (hbaseVersion.equalsIgnoreCase("11x")) {
                return "com.alibaba.datax.plugin.writer.hbasebulkwriter2_11.WapperHBaseBulker";
            } else {
                throw new WapperBulkLoadException(String.format("错误的上游hbaseBulkWriter任务的hbase版本(%s),因此wapperBulkLoad不能执行,dscJobId=%s,dscJobVersion=%s", hbaseVersion, skynetWapper.getFileId(), skynetWapper.getFileVersion()));
            }
        }

        public boolean validate() {
            boolean result = false;
            LOG.info(String.format("HBaseBulkLoadWorker #validate start. fileId=%s,fileVersion=%s,skynetTaskid=%s", skynetWapper.getFileId(), skynetWapper.getFileVersion(), skynetWapper.getTaskId()));

            try {
                classLoaderSwapper.setCurrentThreadClassLoader(classLoader);

                Class involve = this.classLoader.loadClass(getClassName());
                this.invokeInstance = involve.newInstance();
                Method init = involve.getMethod("init", String.class, String.class, String.class);
                init.invoke(invokeInstance, this.hbaseTable, this.hbaseConfigStr, getHdfsOutPut());

                this.doBulk = involve.getMethod("doBulk");
                result = true;
            } catch (ClassNotFoundException e) {
                errorMessage = "未找到class[" + getClassName() + "]," + e.getMessage();
                LOG.error(errorMessage, e);
            } catch (NoSuchMethodException e) {
                errorMessage = "未找到Method[" + getClassName() + "]," + e.getMessage();
                LOG.error(errorMessage, e);
            } catch (Exception e) {
                errorMessage = "HBaseBulkLoadWorker #validate异常:" + e.getMessage();
                LOG.error(errorMessage, e);
            } finally {
                classLoaderSwapper.restoreCurrentThreadClassLoader();
            }

            LOG.info(String.format("HBaseBulkLoadWorker #validate result=%s. fileId=%s,fileVersion=%s,skynetTaskid=%s", result, skynetWapper.getFileId(), skynetWapper.getFileVersion(), skynetWapper.getTaskId()));
            return result;
        }

        @Override
        public void run() {
            LOG.info(String.format("HBaseBulkLoadWorker #run start. fileId=%s,fileVersion=%s,skynetTaskid=%s", skynetWapper.getFileId(), skynetWapper.getFileVersion(), skynetWapper.getTaskId()));
            try {
                classLoaderSwapper.setCurrentThreadClassLoader(classLoader);
                this.doBulk.invoke(this.invokeInstance);
                this.isSuccess = true;
            } catch (Exception e) {
                errorMessage = "执行bulkLoad异常: " + e.getMessage();
                LOG.error(errorMessage, e);
            } finally {
                classLoaderSwapper.restoreCurrentThreadClassLoader();
            }
            LOG.info(String.format("HBaseBulkLoadWorker #run result=%s. fileId=%s,fileVersion=%s,skynetTaskid=%s", isSuccess, skynetWapper.getFileId(), skynetWapper.getFileVersion(), skynetWapper.getTaskId()));
        }

        @Override
        public String getHbaseInfo(boolean printError) {
            StringBuilder sb = new StringBuilder();
            sb.append("[ hbaseTalbe=" + this.hbaseTable);
            sb.append(";");
            sb.append("skynetTaskId=" + this.skynetWapper.getTaskId());
            sb.append(";");
            sb.append("dscJobId=" + this.skynetWapper.getFileId());
            sb.append(";");
            sb.append("dscJobVersion=" + this.skynetWapper.getFileVersion() + "]");
            if(printError) {
                sb.append("errorMsg=" + this.errorMessage);
            }
            return sb.toString();
        }

        public boolean isSuccess() {
            return isSuccess;
        }
    }
}
