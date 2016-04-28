package com.alibaba.datax.plugin.writer.tairwriter;

import com.taobao.tair.impl.mc.MultiClusterTairManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        /**
         * for k3:https://k3.alibaba-inc.com/issue/8147814?versionId=1114264
         * 取消tair的client并发控制.对于datax来说40Channel*40个并发,最大是1600.因此设置2048.相当于取消了限制.会造成一定的上下文切换.
         * 将来对于需要配置40channel的tairWriter,采用分布式. 因为task无法获取到channel,因此选择简单处理
         */
        mcTairManager.setMaxWaitThread(2048);

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
