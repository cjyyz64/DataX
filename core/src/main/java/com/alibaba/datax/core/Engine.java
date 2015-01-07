package com.alibaba.datax.core;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine {
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    /* check job model (job/task) first */
    public void start(Configuration allConf) {

        // 绑定column转换信息
        ColumnCast.bind(allConf);

        /**
         * 初始化PluginLoader，可以获取各种插件配置
         */
        LoadUtil.bind(allConf);

        boolean isJob = !("taskGroup".equalsIgnoreCase(allConf
                .getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));

        AbstractContainer container;
        if (isJob) {
            container = new JobContainer(allConf);
        } else {
            container = new TaskGroupContainer(allConf);
        }

        container.start();
    }

    private static String copyRight() {
        String title = "\nDataX, From Alibaba ! \nCopyright (C) 2010-2015, Alibaba Group. All Rights Reserved.\n";
        return title;
    }

    // 注意屏蔽敏感信息
    private static String filterJobConfiguration(
            final Configuration configuration) {
        Configuration jobConf = configuration.getConfiguration("job.content")
                .clone();

        Set<String> keys = jobConf.getKeys();
        for (final String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key,
                    "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && jobConf.get(key) instanceof String) {
                jobConf.set(key, jobConf.getString(key).replaceAll(".", "*"));
            }
        }

        return jobConf.beautify();
    }

    public static void entry(final String[] args) throws Throwable {
        Options options = new Options();
        options.addOption("job", true, "Job Config .");
        options.addOption("mode", true, "Job Runtime Mode.");

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);

        String jobPath = cl.getOptionValue("job");
        String jobMode = cl.getOptionValue("mode");

        Configuration configuration = ConfigParser.parse(jobPath);
        if (StringUtils.isNotBlank(jobMode)) {
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, jobMode);
        }

        LOG.info("\n" + Engine.copyRight());

        LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");

        LOG.debug(configuration.toJSON());

        ConfigurationValidate.doValidate(configuration);
        Engine engine = new Engine();
        try {
            engine.start(configuration);
        } catch (Throwable e) {
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        try {
            Engine.entry(args);
        } catch (Throwable e) {
            exitCode = 1;
            LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n" + ExceptionTracker.trace(e));

            if (e instanceof DataXException) {
                DataXException tempException = (DataXException) e;
                ErrorCode errorCode = tempException.getErrorCode();
                if (errorCode instanceof FrameworkErrorCode) {
                    FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
                    exitCode = tempErrorCode.toExitValue();
                }
            }

            System.exit(exitCode);
        }

        System.exit(exitCode);
    }

}
