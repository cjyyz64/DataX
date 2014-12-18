package com.alibaba.datax.core.common;

import org.apache.commons.lang.StringUtils;

/**
 * Date: 2014/12/18 10:16
 *
 * @author tianjin.lp <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public enum ExecuteMode {

    LOCAL("local"),
    DISTRIBUTE("distribute"),
    STANDALONE("standalone");

    private String value;

    ExecuteMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static boolean isLocal(String mode) {
        return StringUtils.equalsIgnoreCase(LOCAL.getValue(), mode);
    }

    public static boolean isDistribute(String mode) {
        return StringUtils.equalsIgnoreCase(DISTRIBUTE.getValue(), mode);
    }

    public static boolean isStandAlone(String mode) {
        return StringUtils.equalsIgnoreCase(STANDALONE.getValue(), mode);
    }
}
