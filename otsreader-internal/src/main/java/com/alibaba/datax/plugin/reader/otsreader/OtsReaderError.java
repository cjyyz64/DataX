package com.alibaba.datax.plugin.reader.otsreader;

import com.alibaba.datax.common.spi.ErrorCode;

public class OtsReaderError implements ErrorCode {
    
    private String code;
    
    private String description;
    
    // TODO
    // 这一块需要DATAX来统一定义分类， OTS基于这些分类在细化
    // 所以暂定两个基础的Error Code，其他错误统一使用OTS的错误码和错误消息
    
    public final static OtsReaderError ERROR = new OtsReaderError(
            "OtsReaderError", 
            "OTS Reader Error");
    public final static OtsReaderError INVALID_PARAM = new OtsReaderError(
            "OtsReaderInvalidParameter", 
            "OTS Reader invalid parameter error");
    
    public OtsReaderError (String code) {
        this.code = code;
        this.description = code;
    }
    
    public OtsReaderError (String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
    
    @Override
    public String toString() {
        return "[ code:" + this.code + ", message:" + this.description + "]";
    }
}
