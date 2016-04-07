package com.alibaba.datax.plugin.writer.oceanbasev10writer;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class oceanbasev10writer extends Writer {

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Writer 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     *                          Task类init-->prepare-->startWrite-->post-->destroy
     *                          Task类init-->prepare-->startWrite-->post-->destroy
     *
     *                                                                            Job类post-->destroy
     * </pre>
     */
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration jobConfig = null;

        @Override
        public void init() {
            this.jobConfig = super.getPluginJobConf();

            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常在这里对用户的配置进行校验：是否缺失必填项？有无错误值？有没有无关配置项？...
             * 并给出清晰的报错/警告提示。校验通常建议采用静态工具类进行，以保证本类结构清晰。
             */

        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常采用工具静态类完成把 Job 配置切分成多个 Task 配置的工作。
             * 这里的 mandatoryNumber 是强制必须切分的份数。
             */

            return new ArrayList<Configuration>();
        }

        @Override
        public void post() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常配合 Job 中的 post() 方法一起完成 Job 的资源释放。
             */
        }

    }

    public static class Task extends Writer.Task {

        private Configuration taskConfig;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();

            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：此处通过对 taskConfig 配置的读取，进而初始化一些资源为 startWrite()做准备。
             */
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：如果 Task 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：此处适当封装确保简洁清晰完成数据写入工作。
             */
        }

        @Override
        public void post() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：如果 Task 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：通常配合Task 中的 post() 方法一起完成 Task 的资源释放。
             */
        }

    }

}