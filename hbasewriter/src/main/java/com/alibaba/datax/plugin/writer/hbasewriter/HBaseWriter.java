package com.alibaba.datax.plugin.writer.hbasewriter;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;

import static com.alibaba.datax.plugin.writer.hbasewriter.HBaseWriterErrorCode.*;

public class HBaseWriter extends Writer {
	public static final class Job extends Writer.Job {
		private final Logger log = LoggerFactory.getLogger(this.getClass());

        private Configuration originalConfig;
        private String zips;
        private String zport;
        private String tableName;
        private String columnFamily;
        private int batchrows;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			this.zips = originalConfig.getNecessaryValue(Const.ZOOKEEPER_SERVER_HOSTS, ZIPS_CONFIG_ERROR);
			this.zport = originalConfig.getNecessaryValue(Const.ZOOKEEPER_CLIENT_PORT, ZPORT_CONFIG_ERROR);
			this.tableName = originalConfig.getNecessaryValue(Const.TABLE_NAME, TABLE_NAME_CONFIG_ERROR);
			this.columnFamily = originalConfig.getNecessaryValue(Const.COLUMN_FAMILY, COLUMN_FAMILY_CONFIG_ERROR);
			this.batchrows = originalConfig.getInt(Const.BATCH_ROWS, 100);
		}

		@Override
		public void destroy() {
			
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			List<Configuration> list = Lists.newArrayList();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
		}
        
	} //public static final class Job extends Writer.Job
	
	public static class Task extends Writer.Task {
		private final Logger log = LoggerFactory.getLogger(this.getClass());

        private Configuration sliceConfig;
        private String zips;
        private String zport;
        private String tableName;
        private String columnFamily;
        private int batchrows;
        
        private HBaseClient hbaseClient;
        private List<String> colNames = new ArrayList<String>();

		@Override
		public void init() {
			this.sliceConfig = super.getPluginJobConf();
			this.zips = sliceConfig.getNecessaryValue(Const.ZOOKEEPER_SERVER_HOSTS, ZIPS_CONFIG_ERROR);
			this.zport = sliceConfig.getNecessaryValue(Const.ZOOKEEPER_CLIENT_PORT, ZPORT_CONFIG_ERROR);
			this.tableName = sliceConfig.getNecessaryValue(Const.TABLE_NAME, TABLE_NAME_CONFIG_ERROR);
			this.columnFamily = sliceConfig.getNecessaryValue(Const.COLUMN_FAMILY, COLUMN_FAMILY_CONFIG_ERROR);
			this.batchrows = sliceConfig.getInt(Const.BATCH_ROWS, 100);
			if(batchrows > 1000) {
				log.warn("batchrows is gt 1000, decreased to 1000");
        		batchrows = 1000;
        	}
			
			List<Object> indexList = sliceConfig.getList(Const.COLUMN_NAMES);
			//默认第一列为rowkey主键字段
			if (indexList == null || indexList.size() <= 1) {
                throw DataXException.asDataXException(COLUMN_NAMES_CONFIG_ERROR, COLUMN_NAMES_CONFIG_ERROR.getDescription());
            }
			if (indexList != null && indexList.size() > 0) {
                for (Object index : indexList) {
                    this.colNames.add(index.toString());
                }
            }
			
			hbaseClient = new HBaseClient();
			try {
				hbaseClient.init(zips, zport, tableName);
			} catch (Exception e) {
				log.error("init table "+tableName+" failed.", e);
			}
		}

		@Override
		public void destroy() {
			if(hbaseClient != null) {
				try {
					hbaseClient.close();
				} catch (Exception e) {
					log.error("close table "+tableName+" failed.", e);
				}
			}
		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			int ok = 0;
            int count = 0;
            Record record = null;
            
            //除了第一列rowkey主键外必须还有其他字段
            if(colNames != null && colNames.size() > 1) {
            	List<HBaseCell> cells = new ArrayList<HBaseCell>();
            	
            	while((record = lineReceiver.getFromReader()) != null) {
                	if(colNames.size() != record.getColumnNumber()) {
                		throw DataXException.asDataXException(ILLEGAL_VALUES_ERROR, ILLEGAL_VALUES_ERROR.getDescription() +
                				"读出字段个数:" + record.getColumnNumber() + " " + "配置字段个数:" + colNames.size());
                	}
                	
                	String rowKey = record.getColumn(0).asString();
                	if(rowKey != null && !"".equals(rowKey)) {
                		//loop the cells in the row
                		for(int i = 1; i < colNames.size(); i++) {
                    		String col = colNames.get(i);
                    		String value = record.getColumn(i).asString();
                    		HBaseCell cell = new HBaseCell();
                    		cell.setRowKey(rowKey);
                    		cell.setColf(columnFamily);
                    		cell.setCol(col);
                    		cell.setValue(value);
                    		cells.add(cell);
                    	}
                		
                		//add one row
                		count++;
                		ok++;
                	} //if(rowKey != null && !"".equals(rowKey))
                	
                	
                	if(count >= batchrows) {
                		try {
							hbaseClient.put(tableName, cells);
						} catch (Exception e) {
							log.error("put table "+tableName+" failed.", e);
							cells.clear();
							break;
						}
                		cells.clear();
                		count = 0;
                	}
                } //while((record = lineReceiver.getFromReader()) != null)
            	
            	if(cells != null && cells.size() > 0) {
            		try {
						hbaseClient.put(tableName, cells);
					} catch (Exception e) {
						log.error("put table "+tableName+" failed.", e);
					}
            	}
            	
            	log.info(ok + " rows are successfully inserted into the table " + tableName);
        	} //if(colNames != null && colNames.size() > 1)
		} //public void startWrite(RecordReceiver lineReceiver)
        
	} //public static class Task extends Writer.Task
}
