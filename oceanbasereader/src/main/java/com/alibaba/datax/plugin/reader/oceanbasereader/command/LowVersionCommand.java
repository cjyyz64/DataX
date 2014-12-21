package com.alibaba.datax.plugin.reader.oceanbasereader.command;

import com.alibaba.datax.plugin.reader.oceanbasereader.utils.OBDataSource;
import com.alibaba.datax.plugin.reader.oceanbasereader.utils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;

public class LowVersionCommand implements Command {

	private Logger log = LoggerFactory.getLogger(LowVersionCommand.class);

	@Override
	public void execute(final Context context) throws Exception {
		log.info("OceanBaseJDBCReader start to query. Target OceanBase Version 0.4.");
		String sql = context.originalSQL();
		OBDataSource.execute(context.url(), sql, new ResultSetHandler<Void>() {
            @Override
            public Void callback(ResultSet result) throws Exception {
                context.sendToWriter(result);
                return null;
            }
        });
		log.info("OceanBaseJDBCReader work complete.");
	}

}