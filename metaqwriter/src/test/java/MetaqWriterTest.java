import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;

/**
 * Created by jianying.wcj on 2015/3/22 0022.
 */
@RunWith(LoggedRunner.class)
public class MetaqWriterTest extends BasicWriterPluginTest {

	
	 @TestLogger(log = "测试basic0.json")
	 @Test
	public void testBasic0() {
		int readerSliceNumber = 1;
		super.doWriterTest("basic0.json", readerSliceNumber);
		
	}
	 
	@Override
	protected List<Record> buildDataForWriter() {

		List<Record> list = new ArrayList<Record>();
		Record r = new DefaultRecord();
		r.addColumn(new StringColumn("hello"));
		r.addColumn(new DateColumn(System.currentTimeMillis()));
		r.addColumn(new LongColumn(1000));
		list.add(r);
		return list;

	}

	@Override
	protected String getTestPluginName() {
		// TODO Auto-generated method stub
		  return "metaqwriter";
	}
	
	
}
