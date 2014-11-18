package com.alibaba.datax.plugin.reader.oceanbasereader.command;

public interface Command {

	public abstract void execute(Context context) throws Exception;
}