package com.alibaba.datax.plugin.reader.otsreader.model;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.Constant;
import com.alibaba.datax.plugin.reader.otsreader.utils.ParamChecker;
import com.aliyun.openservices.ots.internal.model.TimeRange;

public class OTSMultiVersionConf {

    private TimeRange timeRange = null;
    private int maxVersion = -1;

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    public int getMaxVersion() {
        return maxVersion;
    }

    public void setMaxVersion(int maxVersion) {
        this.maxVersion = maxVersion;
    }

    public static OTSMultiVersionConf load(Configuration param) throws OTSCriticalException  {
        OTSMultiVersionConf conf = new OTSMultiVersionConf();
        conf.setTimeRange(ParamChecker.checkTimeRangeAndGet(param));
        conf.setMaxVersion(param.getInt(Constant.KEY.MAX_VERSION, Constant.VALUE.MAX_VERSION));
        return conf;
    }
}
