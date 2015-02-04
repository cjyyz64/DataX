import com.alibaba.datax.plugin.writer.adswriter.AdsException;
import com.alibaba.datax.plugin.writer.adswriter.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.TableMetaHelper;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Created by judy.lt on 2015/2/3.
 */
public class AdsWriteUnitTest {
    @Test
    public void ads(){
//        String adsUrl="10.101.90.23:9999";
        String adsUrl="10.101.92.40:9999";
        String userName="gq5FDS2IgSWqXzTu";
        String password="xNXmuBr4dvn3BNLLzWZEAerpHqREto";
        String schema="btest";
        String adsTable="builder_test_table2";
        int lifeCycle = 2;
        AdsHelper adsHelper = new AdsHelper(adsUrl,userName,password,schema);
        try{
            TableInfo tableInfo = adsHelper.getTableInfo(adsTable);
            TableMeta tableMeta = TableMetaHelper.createTempODPSTable(tableInfo, lifeCycle);
            String sql = tableMeta.toDDL();
            System.out.println(sql);
            assertNotNull(sql);
        }catch (AdsException e){
            System.out.println(e);
            assertNull(e);


    }

    }
}
