package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import java.sql.ResultSet;

public interface ResultSetHandler<T> {

	public abstract T callback(ResultSet result) throws Exception;

}