package com.alibaba.datax.hook.dqc;

import com.taobao.dqc.common.entity.DataSourceType;

/**
 * Created by xiafei.qiuxf on 14/12/19.
 */
public class DQCCheckInfo {
    private Integer skynetId;
    private String skynetOnDuty;
    private String skynetBizDate;
    private String skynetSysEnv;

    private DataSourceType dataSourceType;

    private String project;
    private String table;
    private String partition;

    /* 一共读出的行数 */
    private Long totalReadRecord;
    /* 一共失败的行数，包括读写 */
    private Long totalFailedRecord;

    public Integer getSkynetId() {
        return skynetId;
    }

    public void setSkynetId(Integer skynetId) {
        this.skynetId = skynetId;
    }

    public String getSkynetOnDuty() {
        return skynetOnDuty;
    }

    public void setSkynetOnDuty(String skynetOnDuty) {
        this.skynetOnDuty = skynetOnDuty;
    }

    public String getSkynetBizDate() {
        return skynetBizDate;
    }

    public void setSkynetBizDate(String skynetBizDate) {
        this.skynetBizDate = skynetBizDate;
    }

    public String getSkynetSysEnv() {
        return skynetSysEnv;
    }

    public void setSkynetSysEnv(String skynetSysEnv) {
        this.skynetSysEnv = skynetSysEnv;
    }

    public DataSourceType getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(DataSourceType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public Long getTotalReadRecord() {
        return totalReadRecord;
    }

    public void setTotalReadRecord(Long totalReadRecord) {
        this.totalReadRecord = totalReadRecord;
    }

    public Long getTotalFailedRecord() {
        return totalFailedRecord;
    }

    public void setTotalFailedRecord(Long totalFailedRecord) {
        this.totalFailedRecord = totalFailedRecord;
    }

    public Long getTotalSuccessRecord() {
        return totalReadRecord - totalFailedRecord;
    }
}
