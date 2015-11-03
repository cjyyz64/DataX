package com.alibaba.datax.plugin.writer.metaqwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by zeqi.cw on 2015/10/20
 */
public enum MetaqWriterErrorCode implements ErrorCode {

	REQUIRED_VALUE("MetaqWriter-01", "您缺失了必须填写的参数值."),
	METAQWRITER_ERROR("MetaqWriter-02", "Metaq发生错误退出."),
	MQClIENT_EXCEPTION("CREATE MetaProducer ERROR","创建MetaProducer失败");
	
	
    private final String code;

    private final String description;

    private MetaqWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
