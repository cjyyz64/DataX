package com.alibaba.datax.plugin.writer.oceanbasewriter.strategy;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.SlavePluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasewriter.Key;
import com.alibaba.datax.plugin.writer.oceanbasewriter.OceanbaseErrorCode;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.OBDataSource;
import com.alibaba.datax.plugin.writer.oceanbasewriter.utils.ResultSetHandler;

import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Context {
	
	public final RecordReceiver recordReceiver;
	private final Configuration configuration;
    private final SlavePluginCollector slavePluginCollector;
	public static volatile boolean permit = true;
	public static final long daemon_check_interval = 30 * 1000;
	
	public Context(Configuration configuration, RecordReceiver recordReceiver, SlavePluginCollector slavePluginCollector) {
		this.recordReceiver = recordReceiver;
		this.configuration = configuration;
        this.slavePluginCollector = slavePluginCollector;
	}

	public String url(){
		return this.configuration.getString(Key.CONFIG_URL);
	}
	
	public boolean useDsl(){
		return !"".equals(this.configuration.getString(Key.ADVANCE, ""));
	}
	
	public String table(){
		return this.configuration.getString(Key.TABLE);
	}
	
	public int batch(){
		return this.configuration.getInt(Key.BATCH_SIZE, 1000);
	}
	
	public List<String> normal(){
		return this.configuration.getList(Key.COLUMNS, String.class);
	}
	
	public String dsl(){
		return this.configuration.getString(Key.ADVANCE);
	}

    public String writeMode(){
        return this.configuration.getString(Key.WRITE_MODE);
    }
	
	public Map<String, String> columnType() {
		ResultSetHandler<Map<String, String>> handler = new ResultSetHandler<Map<String, String>>() {
			@Override
			public Map<String, String> callback(ResultSet result)
					throws Exception {
				Map<String, String> map = new LinkedHashMap<String, String>();
				while (result.next()) {
					String name = result.getString("field").toLowerCase();
					String type = result.getString("type").toLowerCase();
					if(type.equalsIgnoreCase("createtime") || type.equalsIgnoreCase("modifytime")){
						type = "timestamp";
					}
					map.put(name, type);
				}
				return map;
			}
		};
		try {
			return OBDataSource.executeQuery(String.format("desc %s", table()), handler);
		} catch (Exception e) {
			throw DataXException.asDataXException(OceanbaseErrorCode.DESC,e);
		}
	}
	
	public void reportFail(Record record,Exception e){
        this.slavePluginCollector.collectDirtyRecord(record,e);
	}
	
	public String badFile(){
		return this.configuration.getString(Key.BAD_FILE, "");
	}


	public long activeMemPercent(){
		return this.configuration.getInt(Key.ACTIVE_MEM_PERCENT, 60);
	}

}