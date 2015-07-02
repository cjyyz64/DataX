package com.alibaba.datax.plugin.writer.tairwriter;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;

import com.taobao.tair.impl.DefaultTairManager;
import com.taobao.tair.impl.mc.MultiClusterTairManager;

public final class TairClientManager {
    private static MultiClusterTairManager MC_TAIR_MANAGER;
    private static final Logger LOG = LoggerFactory.getLogger(TairClientManager.class);

  public static synchronized MultiClusterTairManager getInstance(String configId,
      String lang, int compressionThreshold, int timeout) {
        if (null == TairClientManager.MC_TAIR_MANAGER) {
            register(configId, lang, compressionThreshold, timeout);
        }
        return TairClientManager.MC_TAIR_MANAGER;
  }

    private static void register(String configId,
            String lang, int compressionThreshold, int timeout) {

        MultiClusterTairManager mcTairManager = new MultiClusterTairManager();
        mcTairManager.setConfigID(configId);
        mcTairManager.setDynamicConfig(true);
        mcTairManager.setTimeout(timeout);
        mcTairManager.setHeader("c++".equalsIgnoreCase(lang) ? false : true);
        if ("c++".equalsIgnoreCase(lang)) {
            mcTairManager.setCompressionThreshold(1000000);
        } else {
            mcTairManager.setCompressionThreshold(compressionThreshold);
        }
        mcTairManager.init();
        TairClientManager.MC_TAIR_MANAGER = mcTairManager;
        return;
    }
}
