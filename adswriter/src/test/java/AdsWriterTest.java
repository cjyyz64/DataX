import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.test.simulator.BasicWriterPluginTest;
import com.alibaba.datax.test.simulator.junit.extend.log.LoggedRunner;
import com.alibaba.datax.test.simulator.junit.extend.log.TestLogger;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * Created by judy.lt on 2015/2/2.
 */
@RunWith(LoggedRunner.class)
public class AdsWriterTest extends BasicWriterPluginTest {

    @TestLogger(log = "ADS Writer Basic Test")
    @Test
    public void testBasic0() {
        int readerSliceNumber = 1;
        super.doWriterTest("basic0.json", readerSliceNumber);
    }

    @Override
    protected List<Record> buildDataForWriter() {
        return null;
    }

    @Override
    protected String getTestPluginName() {
        return null;
    }
}
