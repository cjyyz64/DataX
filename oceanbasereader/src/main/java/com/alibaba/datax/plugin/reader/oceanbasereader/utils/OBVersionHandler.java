package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import java.sql.ResultSet;

public class OBVersionHandler implements ResultSetHandler<Boolean>{

    public static final String version = "show variables like 'version_comment'";
    @Override
    public Boolean callback(ResultSet result) throws Exception {
        result.next();
        String version = result.getString("value");
        return version.contains("0.4");
    }
}
