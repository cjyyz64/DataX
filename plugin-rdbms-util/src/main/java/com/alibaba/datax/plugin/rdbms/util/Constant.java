package com.alibaba.datax.plugin.rdbms.util;

public final class Constant {
    static final int TIMEOUT_SECONDS = 3;
    static final int MAX_TRY_TIMES = 4;

    static final String MYSQL_DATABASE = "Unknown database";
    static final String MYSQL_CONNEXP = "CommunicationsException";
    static final String MYSQL_ACCDENIED = "Access denied";

    static final String ORACLE_DATABASE = "ORA-12505";
    static final String ORACLE_CONNEXP = "The Network Adapter could not establish the connection";
    static final String ORACLE_ACCDENIED = "ORA-01017";

}
