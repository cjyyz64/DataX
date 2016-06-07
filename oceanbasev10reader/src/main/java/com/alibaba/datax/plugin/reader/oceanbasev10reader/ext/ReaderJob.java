package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.OBUtils;

public class ReaderJob extends CommonRdbmsReader.Job{

	public ReaderJob() {
		super(OBUtils.DATABASE_TYPE);
	}

	@Override
	public List<Configuration> split(Configuration originalConfig, int adviceNumber) {
		// TODO 以后OB要按partition来切分
		return super.split(originalConfig, adviceNumber);
	}
}
