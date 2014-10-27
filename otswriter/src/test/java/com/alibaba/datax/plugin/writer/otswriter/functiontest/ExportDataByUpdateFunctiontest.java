package com.alibaba.datax.plugin.writer.otswriter.functiontest;

/**
 * 通过Update的方式导入数据到OTS中，验证数据的正确性
 * @author redchen
 *
 */
public class ExportDataByUpdateFunctiontest {
    // 混合PK数据导入测试，主要测试Writer在各种PK组合下功能是否正常
    
    // 原表中已经存在100行数据，包含col0~col10这10个列，
    // datax导入新的数据包含（col5~col15）数据校验的时候检查（col0~col4为旧的数据，col5到col15为新的数据）
}
