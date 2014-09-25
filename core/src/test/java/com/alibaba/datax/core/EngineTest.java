package com.alibaba.datax.core;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.util.LoadUtil;
import com.alibaba.datax.core.faker.FakeMasterContainer;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.CoreConstant;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by jingxing on 14-9-25.
 */
public class EngineTest extends CaseInitializer {
    private Configuration configuration;

    @Before
    public void setUp() {
        String path = EngineTest.class.getClassLoader()
                .getResource(".").getFile();

        this.configuration = ConfigParser.parse(path + File.separator
                + "all.json");
        LoadUtil.bind(this.configuration);
    }

    @Test
    public void testMainMaster() throws Exception {
        this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_MASTER_CLASS,
                FakeMasterContainer.class.getName());
        String jobConfig = this.configuration.toString();

        String jobFile = "./testJob.json";
        FileWriter writer = new FileWriter(jobFile);
        writer.write(jobConfig);
        writer.flush();
        writer.close();

        String[] args = {"-job", jobFile};
        Engine.main(args);
    }



}
