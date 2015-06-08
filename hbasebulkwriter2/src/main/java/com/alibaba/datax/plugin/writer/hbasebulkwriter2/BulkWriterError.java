package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import com.alibaba.datax.common.spi.ErrorCode;

public class BulkWriterError implements ErrorCode{
  private String code;
  private String describe;
  
  public static final String CONFIG_MISSING_CODE = "10001";
  public static final String CONFIG_ILLEGAL_CODE = "10002";
  
  public static final String RUNTIME_CODE = "20000";
  public static final String RUNTIME_CAST_CODE = "20001";
  public static final String RUNTIME_ILLEGAL_TIME_CODE = "20002";
  public static final String HBASE_ERROR_CODE = "50000";
  
  public static final String HDFS_JAR_LOAD_CODE = "60001";
  public static final String CLASS_LOADER_CODE = "60002";
  
  public static BulkWriterError IO = new BulkWriterError(CLASS_LOADER_CODE, "fail on reflection");
  public static BulkWriterError HDFS_JAR_LOAD = new BulkWriterError(HDFS_JAR_LOAD_CODE, "fail load hdfs jar");

  public static BulkWriterError RUNTIME = new BulkWriterError(RUNTIME_CODE, "runtime error");
  public static BulkWriterError CONFIG_MISSING = new BulkWriterError(CONFIG_MISSING_CODE, "config missing");
  public static BulkWriterError CONFIG_ILLEGAL = new BulkWriterError(CONFIG_ILLEGAL_CODE, "config illegal");
  public static BulkWriterError RUNTIME_CAST = new BulkWriterError(RUNTIME_CAST_CODE, "runtime cast exception");
  public static BulkWriterError RUNTIME_ILLEGAL_TIME = new BulkWriterError(RUNTIME_ILLEGAL_TIME_CODE);
  public static BulkWriterError HBASE_ERROR = new BulkWriterError(HBASE_ERROR_CODE);
  
  public BulkWriterError(String code, String describe) {
    this.code = code;
    this.describe = describe;
  }
  
  public BulkWriterError(String code) {
    this(code, "slave-side error");
  }

  @Override
  public String toString() {
   return String.format("Code:[%s], Description:[%s]. ", this.code, 
    this.describe);
  }

  @Override
  public String getCode() {
    return this.code;
  }

  @Override
  public String getDescription() {
    return this.describe;
  }
}
