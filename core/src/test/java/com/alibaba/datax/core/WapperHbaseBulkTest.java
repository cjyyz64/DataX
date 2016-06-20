package com.alibaba.datax.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.WapperHbaseBulk.HBaseBulkLoadWorker;
import com.alibaba.datax.core.WapperHbaseBulk.SkynetBulkloadWapperFatherJobIdVersion;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.fastjson.JSON;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/6/19.
 */
public class WapperHbaseBulkTest extends CaseInitializer {

    WapperHbaseBulk wapperHbaseBulk = new WapperHbaseBulk();

    @Test
    public void testGetJobId() throws Exception {
        Method getList = WapperHbaseBulk.class.getDeclaredMethod("getJobId", String.class);
        getList.setAccessible(true);

        String testList = "[{\"fileId\":1001,\"fileVersion\":1002},{\"fileId\":1003,\"fileVersion\":1004},{\"fileId\":1005,\"fileVersion\":1006}]";
        List<SkynetBulkloadWapperFatherJobIdVersion> res = (List<SkynetBulkloadWapperFatherJobIdVersion>) getList.invoke(wapperHbaseBulk, testList);

        Assert.assertEquals(res.size(), 3);
        for (SkynetBulkloadWapperFatherJobIdVersion jobId : res) {
            if (jobId.getFileId() == 1001L) {
                Assert.assertEquals(jobId.getFileVersion().longValue(), 1002L);
            } else if (jobId.getFileId() == 1003L) {
                Assert.assertEquals(jobId.getFileVersion().longValue(), 1004L);
            } else if (jobId.getFileId() == 1005L) {
                Assert.assertEquals(jobId.getFileVersion().longValue(), 1006L);
            } else {
                throw new Exception("error");
            }
        }
        System.out.println(JSON.toJSONString(res));

    }

    @Test
    public void getJobConfig() throws Exception {
        Method getJobConfig = WapperHbaseBulk.class.getDeclaredMethod("getJobConfig", Configuration.class, Long.class, Long.class);
        getJobConfig.setAccessible(true);

        Configuration core = Configuration.from(new File(CoreConstant.DATAX_CONF_PATH));

        List<String> plugins = new ArrayList<String>();
        plugins.add("hbasebulkwriter2");
        plugins.add("hbasebulkwriter2_11x");
        Configuration pluginsConfig = ConfigParser.parsePluginConfig(plugins);

        Configuration jobConfig = (Configuration) getJobConfig.invoke(wapperHbaseBulk, core, 2977609L, 6862462L);

        Assert.assertTrue(jobConfig != null);
        Assert.assertEquals(jobConfig.getString("data.writer.hbaseVersion"), "094x");
        Assert.assertEquals(jobConfig.getString("data.writer.hbaseOutput"), "/test123");
        Assert.assertEquals(jobConfig.getString("data.writer.hbaseBulkLoadControl"), "false");

        System.out.println(jobConfig.getString("data.writer.configuration"));
        Map<String, Object> hbaseConfig = jobConfig.getMap("data.writer.configuration");
        for (Map.Entry<String, Object> entry : hbaseConfig.entrySet()) {
            System.out.println(entry.getKey() + " ==> " + String.valueOf(entry.getValue()));
        }
        System.out.println(jobConfig.getMap("data.writer.configuration"));
    }
}