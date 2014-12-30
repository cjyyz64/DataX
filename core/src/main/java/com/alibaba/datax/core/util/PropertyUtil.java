package com.alibaba.datax.core.util;

import com.alibaba.datax.core.util.container.CoreConstant;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by hongjiao.hj on 2014/12/29.
 */
public class PropertyUtil {

    public static Properties prop = new Properties();
    public static Properties getPropertUtil() {
        String path = CoreConstant.DATAX_SECRET_PATH;
        try {
            prop.load(new FileInputStream(path));
            return prop;
        } catch (IOException e) {
            System.out.println("配置文件未找到");
            throw new RuntimeException(e);
        }
    }

}
