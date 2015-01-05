package com.alibaba.datax.hook.dqc;

import com.taobao.dqc.common.entity.DataSourceType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DQCCheckerTest {

    private DQCChecker u;

    @BeforeMethod
    public void setUp() throws Exception {
        u = new DQCChecker(this.getClass().getClassLoader().getResourceAsStream("dqc.properties"));
    }

    @DataProvider(name = "DQC_DP")
    public Object[][] dqcDP() {
        return new Object[][] {
                new Object[]{"datax_test_dqc_1", 10L, 1L, 9L, true},
                new Object[]{"datax_test_dqc_1", 10L, 2L, 8L, false},
                new Object[]{"datax_test_dqc_2", 40L, 2L, 38L, true},
                new Object[]{"datax_test_dqc_2", 40L, 12L, 28L, false},
        };
    }


    @Test(dataProvider = "DQC_DP")
    public void testDQC(final String table,
                        final Long totalRead, final Long totalFailed,
                        Long expectTotalSuccess,
                        boolean pass)
            throws Exception {

        DQCCheckInfo info = new DQCCheckInfo() {
            {
                this.setSkynetId(123);
                this.setSkynetBizDate("20141212");
                this.setSkynetOnDuty("039923");
                this.setSkynetSysEnv("");

                this.setDataSourceType(DataSourceType.Odps);
                this.setProject("autotest");
                this.setTable(table);
                this.setPartition("ds=1");

                this.setTotalReadRecord(totalRead);
                this.setTotalFailedRecord(totalFailed);
            }
        };

        assertEquals(info.getTotalSuccessRecord(), expectTotalSuccess);
        assertEquals(u.doDQCCheck(info), pass);
    }

    @Test
    public void testSkipHive() throws Exception {
        DQCCheckInfo info = new DQCCheckInfo() {
            {
                this.setSkynetId(123);
                this.setSkynetBizDate("20141212");
                this.setSkynetOnDuty("039923");
                this.setSkynetSysEnv("");

                this.setDataSourceType(DataSourceType.Hive);
                this.setProject("autotest");
                this.setTable("a");
                this.setPartition("ds=1");

                this.setTotalReadRecord(10L);
                this.setTotalFailedRecord(1L);
            }
        };
        assertTrue(u.doDQCCheck(info));
    }

    @Test
    public void testSkipSkynet1() throws Exception {
        DQCCheckInfo info = new DQCCheckInfo() {
            {
                this.setSkynetId(123);
                this.setSkynetBizDate("20141212");
                this.setSkynetOnDuty("039923");
//                this.setSkynetSysEnv("");

                this.setDataSourceType(DataSourceType.Odps);
                this.setProject("autotest");
                this.setTable("a");
                this.setPartition("ds=1");

                this.setTotalReadRecord(10L);
                this.setTotalFailedRecord(1L);
            }
        };
        assertTrue(u.doDQCCheck(info));
    }
}