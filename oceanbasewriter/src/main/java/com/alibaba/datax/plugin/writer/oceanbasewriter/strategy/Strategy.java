package com.alibaba.datax.plugin.writer.oceanbasewriter.strategy;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasewriter.column.ColumnMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class Strategy {

	private Helper helper;
	protected Context context;
	protected List<ColumnMeta> columns;
	
	public Strategy(Context context, List<ColumnMeta> columns) {
		this.context = context;
		this.columns = columns;
		this.helper = new Helper();
	}

	protected abstract void write(List<Record> records) throws Exception;

	public void batchWrite() throws Exception {
		final int batch = context.batch();
		while (true) {
			if(!Context.permit){
				Thread.sleep(Context.daemon_check_interval);
			}else{
				List<Record> records = Lists.newArrayListWithExpectedSize(batch);
				for (int index = 0; index < batch; index++) {
					Record record = context.recordReceiver.getFromReader();
					if (record == null) break;
					records.add(record);
				}
				if (records.size() == batch){
                    this.writeWithRetry(records,null);
                }else if (records.isEmpty()){
                    return;
                }else {
                    this.writeWithRetry(records,null);
                    return;
                }
			}
		}
	}

	private void writeWithRetry(List<Record> records,Record original){
		try {
			this.write(records);
		} catch (Exception e) {
			if(original != null){
				helper.handle(original, e);
			}else{
				for(Record record : records){
					this.writeWithRetry(ImmutableList.of(record), record);
				}
			}
		}
	}
	
	public static Strategy instance(Context context, List<ColumnMeta> columns) throws Exception{
        String writeMode = context.writeMode();
        if("delete".equalsIgnoreCase(writeMode)){
            return new DeleteStrategy(context,columns);
        }else {
            return new PrepareStrategy(context, columns);
        }
	}

	private class Helper{
		
		private final int max = 100;
		private final Logger log = LoggerFactory.getLogger(Helper.class);
		private void reportToDatax(Record record,Exception e){
			context.reportFail(record,e);
		}
		
		private void logException(Record record,Exception e){
			if(context.failNumber() < max){
				log.error(record.toString(),e);
			}else if(context.failNumber() == max){
				log.error("Pay attention more bad lines are hidden");
			}
		}
		
		public void handle(Record record,Exception e){
			 reportToDatax(record,e);
			 logException(record,e);
		}
		
	}
	
}