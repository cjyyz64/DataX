package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.odpswriter.Constant;

import java.util.ArrayList;
import java.util.List;

public final class OdpsSplitUtil {

    public static List<Configuration> doSplit(Configuration originalConfig, String uploadId, List<Long> blocks,
                                              int mandatoryNumber) {

        List<Configuration> configs = new ArrayList<Configuration>();
        Configuration tempConf;

        for (int i = 0; i < mandatoryNumber; i++) {
            blocks.add((long) i);

            tempConf = originalConfig.clone();
            tempConf.set(Constant.UPLOAD_ID, uploadId);

            //TODO block id 不能直接指定死
            tempConf.set(Constant.BLOCK_ID, i);
            configs.add(tempConf);
        }

        return configs;
    }

}
