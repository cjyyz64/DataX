import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.adswriter.AdsException;
import com.alibaba.datax.plugin.writer.adswriter.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.AdsWriterErrorCode;
import com.alibaba.datax.plugin.writer.adswriter.TableMetaHelper;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Created by judy.lt on 2015/2/3.
 */
public class AdsWriteUnitTest {
    private final int ODPSOVERTIME = 10000;
    @Test
    public void ads(){
//        String adsUrl="10.101.90.23:9999";
        String adsUrl="10.101.92.40:9999";
        String userName="gq5FDS2IgSWqXzTu";
        String password="xNXmuBr4dvn3BNLLzWZEAerpHqREto";
        String schema="btest";
        String adsTable="builder_test_table2";
        String endPoint = "http://service.odpsstg.aliyun-inc.com/stgnew";
        String accessId = "vw6mmLVuAAa0cHcV";
        String accessKey = "pG4s6hPmEhglCy9szEEdpBUPTvg0JS";
        String project = "autotest_dev";
        int lifeCycle = 2;
        AdsHelper adsHelper = new AdsHelper(adsUrl,userName,password,schema);
        Account odpsAccount = new AliyunAccount(accessId,accessKey);
        Odps odps = new Odps(odpsAccount);
        odps.setEndpoint(endPoint);
        try{
            TableInfo tableInfo = adsHelper.getTableInfo(adsTable);
            TableMeta tableMeta = TableMetaHelper.createTempODPSTable(tableInfo, lifeCycle);
            String sql = tableMeta.toDDL();
            //创建odps表
            Instance instance = SQLTask.run(odps, project, sql, null, null);
            String id = instance.getId();
            boolean terminated = false;
            int time = 0;
            while(!terminated && time < ODPSOVERTIME)
            {
                Thread.sleep(1000);
                terminated = instance.isTerminated();
                time += 1000;
            }
            System.out.println(sql);
            assertNotNull(sql);
        }catch (AdsException e){
            System.out.println(e);
            assertNull(e);
        }catch (OdpsException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED, e);
        } catch (InterruptedException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
        }
    }

    @Test
    public void generateSourcePathTest(){
        String project = "testProject";
        String tmpOdpsTableName = "tmpOdpsTableName";
        String sourcePath = AdsUtil.generateSourcePath(project,tmpOdpsTableName);
        assertNotNull(sourcePath);
    }
    @Test
    public void adsHelpCheckTest() {
        String adsUrl = "10.101.92.40:9999";
        String userName = "gq5FDS2IgSWqXzTu";
        String password = "xNXmuBr4dvn3BNLLzWZEAerpHqREto";
        String schema = "btest";
        AdsHelper adsHelper = new AdsHelper(adsUrl, userName, password, schema);
        String id = "LDDT-dailybuild-btest__builder_test_table2-20150205165450462";
        try {
            adsHelper.checkLoadDataJobStatus(id);
        } catch (AdsException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.TABLE_TRUNCATE_ERROR, e);
        }
    }
}
