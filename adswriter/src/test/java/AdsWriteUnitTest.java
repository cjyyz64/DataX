import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.adswriter.*;
import com.alibaba.datax.plugin.writer.adswriter.ads.TableInfo;
import com.alibaba.datax.plugin.writer.adswriter.load.AdsHelper;
import com.alibaba.datax.plugin.writer.adswriter.load.TableMetaHelper;
import com.alibaba.datax.plugin.writer.adswriter.odps.TableMeta;
import com.alibaba.datax.plugin.writer.adswriter.util.AdsUtil;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

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
        String sourcePath = AdsUtil.generateSourcePath(project,tmpOdpsTableName,null);
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

    @Test
    public void odpsReaderTest(){
        String readerPluginName = "odpsreader";
        String odpsTableName ="test555__table_2up01_14236492863111508";
        boolean isSucess = false;
        if (readerPluginName.equals(AdsWriter.Job.ODPS_READER)){
            isSucess = loadAdsData(odpsTableName);
            System.exit(0);
        }
        assertTrue(isSucess);
    }

    private boolean loadAdsData(String odpsTableName){
        String table = "table_2up01";
        String project = "autotest_dev";
        String partition = "('id','ds=20150108')";
        String sourcePath = AdsUtil.generateSourcePath(project,odpsTableName,null);
        boolean overwrite = true;
        String adsUrl = "10.101.90.23:9999";
        String userName = "gq5FDS2IgSWqXzTu";
        String password = "xNXmuBr4dvn3BNLLzWZEAerpHqREto";
        String schema = "test555";
        AdsHelper adsHelper = new AdsHelper(adsUrl, userName, password, schema);
        try {
            String id = adsHelper.loadData(table,partition,sourcePath,overwrite);
            boolean terminated = false;
            int time = 0;
            while(!terminated)
            {
                Thread.sleep(120000);
                terminated = adsHelper.checkLoadDataJobStatus(id);
            }
            return terminated;
        } catch (AdsException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.ADS_LOAD_TEMP_ODPS_FAILED,e);
        } catch (InterruptedException e) {
            throw DataXException.asDataXException(AdsWriterErrorCode.ODPS_CREATETABLE_FAILED,e);
        }
    }
    /*测试 AdsWriter Plugin的Config.properties*/
    @Test
    public void getPropertiesTest(){
//        String endPoint = PropertyLoader.getString(Key.ODPS_SERVER);
//        String accessId = PropertyLoader.getString(Key.ACCESS_ID);
//        String accessKey = PropertyLoader.getString(Key.ACCESS_KEY);
//        String project = PropertyLoader.getString(Key.PROJECT);
//        assertNotNull(endPoint);
//        assertNotNull(accessId);
//        assertNotNull(accessKey);
//        assertNotNull(project);
        // TODO test
    }

    /*测试把odps reader的partition转意为Ads partition的信息*/
    @Test
    public void TransferOdpsPartitionToAds(){
        String partition1 = "pt=*";
        String partition2 = "pt=1,ds=*";
        String partition3 = "pt=1,ds=hz,dt=3";
        String partition4 = "pt=1,ds=hz,dt=*";
        String partition5 = "pt=*,dt=hz";
        String adsPartition1 = AdsUtil.transferOdpsPartitionToAds(partition1);
        String adsPartition2 = AdsUtil.transferOdpsPartitionToAds(partition2);
        String adsPartition3 = AdsUtil.transferOdpsPartitionToAds(partition3);
        String adsPartition4 = AdsUtil.transferOdpsPartitionToAds(partition4);
        String adsPartition5 = AdsUtil.transferOdpsPartitionToAds(partition5);
        assertEquals(adsPartition1,"");
        assertNotNull(adsPartition2);
        assertNotNull(adsPartition3);
        assertNotNull(adsPartition4);
        assertNotNull(adsPartition5);
    }


    @Test
    public void testAdsInsertMode() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://" + "10.101.90.23:9999" + "/" + "test555" + "?useUnicode=true&characterEncoding=UTF-8";

            Properties connectionProps = new Properties();
            connectionProps.put("user", "gq5FDS2IgSWqXzTu");
            connectionProps.put("password", "xNXmuBr4dvn3BNLLzWZEAerpHqREto");
            Connection connection = DriverManager.getConnection(url, connectionProps);
            Statement statement = connection.createStatement();

            ResultSet rs = statement.executeQuery("show databases");

            String jobId = null;
            while (rs.next()) {
                jobId = rs.getString(1);
                System.out.println(jobId);
            }

            if (jobId == null) {
                throw new AdsException(AdsException.ADS_LOADDATA_JOBID_NOT_AVAIL,
                        "Job id is not available for the submitted LOAD DATA." + jobId, null);
            }
        } catch (Exception e) {

        }



    }

    @Test
    public void testConfchange() {

        Configuration configuration = Configuration.from("{\n" +
                "    \"odps\": {\n" +
                "        \"accessId\": \"xasdfkladslfjsaifw224ysgsa5\",\n" +
                "        \"accessKey\": \"asfjkljfp0w4624twfswe56346212341adsfa3\",\n" +
                "        \"account\": \"xxx@aliyun.com\",\n" +
                "        \"odpsServer\": \"http://service.odpsstg.aliyun-inc.com/stgnew\",\n" +
                "        \"tunnelServer\": \"http://tunnel.odpsstg.aliyun-inc.com\",\n" +
                "        \"accountType\": \"aliyun\",\n" +
                "        \"project\": \"transfer_project\"\n" +
                "    },\n" +
                "    \"writeMode\": \"load\",\n" +
                "    \"url\": \"127.0.0.1:3306\",\n" +
                "    \"schema\": \"schema\",\n" +
                "    \"table\": \"table\",\n" +
                "    \"username\": \"username\",\n" +
                "    \"password\": \"password\",\n" +
                "    \"partition\": \"\",\n" +
                "    \"lifeCycle\": 2,\n" +
                "    \"overWrite\": true\n" +
                "}");
        //Configuration result = AdsUtil.adsConfToRdbmsConf(configuration);
        //System.out.println(result);
    }
}
