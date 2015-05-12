package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import java.util.List;

public interface PreparedStatementHandler {

    public abstract String sql();

    public abstract List<?> parameters();

}
