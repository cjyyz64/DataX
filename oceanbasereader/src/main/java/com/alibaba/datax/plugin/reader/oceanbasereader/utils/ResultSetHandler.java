package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import java.sql.ResultSet;

public interface ResultSetHandler<T> {

	public abstract T callback(ResultSet result) throws Exception;

}