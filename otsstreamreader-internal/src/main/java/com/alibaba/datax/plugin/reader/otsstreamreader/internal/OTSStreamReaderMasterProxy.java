package com.alibaba.datax.plugin.reader.otsstreamreader.internal;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConfig;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.config.OTSStreamReaderConstants;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.CheckpointTimeTracker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.core.OTSStreamReaderChecker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.GsonParser;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.OTSHelper;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.StreamClientHelper;
import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OTSStreamReaderMasterProxy {

    private OTSStreamReaderConfig conf = null;
    private OTS ots = null;

    private static final Logger LOG = LoggerFactory.getLogger(OTSStreamReaderConfig.class);

    public void init(OTSStreamReaderConfig config) throws Exception {
        this.conf = config;

        // Init ots
        ots = OTSHelper.getOTSInstance(conf);

        // 创建Checker
        OTSStreamReaderChecker checker = new OTSStreamReaderChecker(ots, conf);

        // 检查Stream是否开启，选取的时间范围是否可以导出。
        checker.checkStreamEnabledAndTimeRangeOK();

        // 检查StatusTable是否存在，若不存在则创建StatusTable。
        checker.checkAndCreateStatusTableIfNotExist();

        // 删除StatusTable记录的该Stream的原有的Lease信息。防止本次任务受到之前导出任务的影响。
        String streamId = OTSHelper.getStreamDetails(ots, config.getDataTable()).getStreamId();
        StreamClientHelper.clearAllLeaseStatus(ots, config.getStatusTable(), streamId);

        // 删除StatusTable记录的对应EndTime时刻的Checkpoint信息。防止本次任务受到之前导出任务的影响。
        CheckpointTimeTracker checkpointInfoTracker = new CheckpointTimeTracker(ots, config.getStatusTable(), streamId);
        checkpointInfoTracker.clearShardCountAndAllCheckpoints(config.getEndTimestampMillis());
    }

    public List<Configuration> split(int adviceNumber) {
        List<Configuration> configurations = new ArrayList<Configuration>();
        Configuration configuration = Configuration.newDefault();
        configuration.set(OTSStreamReaderConstants.CONF, GsonParser.configToJson(conf));
        configurations.add(configuration);
        return configurations;
    }

    public void close(){
        ots.shutdown();
    }

}
