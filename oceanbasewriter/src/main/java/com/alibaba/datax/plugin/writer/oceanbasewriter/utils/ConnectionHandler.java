package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import java.sql.Connection;
import java.sql.Statement;

public interface ConnectionHandler {

	public abstract Statement callback(Connection connection) throws Exception;

}