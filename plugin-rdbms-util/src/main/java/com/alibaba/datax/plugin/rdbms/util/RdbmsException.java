package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by judy.lt on 2015/6/5.
 */
public class RdbmsException extends DataXException{
    public RdbmsException(ErrorCode errorCode, String message){
        super(errorCode,message);
    }

    public static DataXException asDataXException(DataBaseType dataBaseType,Exception e){
        if (dataBaseType.equals(DataBaseType.MySql)){
            DBUtilErrorCode dbUtilErrorCode = mySqlConnectionErrorAna(e.getMessage());
            return DataXException.asDataXException(dbUtilErrorCode,e);
        }

        if (dataBaseType.equals(DataBaseType.Oracle)){
            DBUtilErrorCode dbUtilErrorCode = oracleConnectionErrorAna(e.getMessage());
            return DataXException.asDataXException(dbUtilErrorCode,e);
        }

        return DataXException.asDataXException(DBUtilErrorCode.CONN_DB_ERROR,e);
    }

    public static DBUtilErrorCode mySqlConnectionErrorAna(String e){
        if (e.contains(Constant.MYSQL_DATABASE)){
            return DBUtilErrorCode.MYSQL_CONN_DB_ERROR;
        }

        if (e.contains(Constant.MYSQL_CONNEXP)){
            return DBUtilErrorCode.MYSQL_CONN_IPPORT_ERROR;
        }

        if (e.contains(Constant.MYSQL_ACCDENIED)){
            return DBUtilErrorCode.MYSQL_CONN_USERPWD_ERROR;
        }

        return DBUtilErrorCode.CONN_DB_ERROR;
    }

    public static DBUtilErrorCode oracleConnectionErrorAna(String e){
        if (e.contains(Constant.ORACLE_DATABASE)){
            return DBUtilErrorCode.ORACLE_CONN_DB_ERROR;
        }

        if (e.contains(Constant.ORACLE_CONNEXP)){
            return DBUtilErrorCode.ORACLE_CONN_IPPORT_ERROR;
        }

        if (e.contains(Constant.ORACLE_ACCDENIED)){
            return DBUtilErrorCode.ORACLE_CONN_USERPWD_ERROR;
        }

        return DBUtilErrorCode.CONN_DB_ERROR;
    }

    public static DataXException asQueryException(DataBaseType dataBaseType, Exception e,String querySql){
        if (dataBaseType.equals(DataBaseType.MySql)){
            DBUtilErrorCode dbUtilErrorCode = mySqlQueryErrorAna(e.getMessage());
            return DataXException.asDataXException(dbUtilErrorCode,querySql+e);
        }else if (dataBaseType.equals(DataBaseType.Oracle)){
            DBUtilErrorCode dbUtilErrorCode = oracleQueryErrorAna(e.getMessage());
            return DataXException.asDataXException(dbUtilErrorCode,querySql+e);
        }else{
            return DataXException.asDataXException(DBUtilErrorCode.READ_RECORD_FAIL,querySql+e);
        }
    }

    public static DBUtilErrorCode mySqlQueryErrorAna(String e){
        if (e.contains(Constant.MYSQL_TABLE_NAME_ERR1) && e.contains(Constant.MYSQL_TABLE_NAME_ERR2)){
            return DBUtilErrorCode.MYSQL_QUERY_TABLE_NAME_ERROR;
        }else if (e.contains(Constant.MYSQL_INSERT_PRI)){
            return DBUtilErrorCode.MYSQL_QUERY_INSERT_PRI_ERROR;
        }else if (e.contains(Constant.MYSQL_COLUMN1) && e.contains(Constant.MYSQL_COLUMN2)){
            return DBUtilErrorCode.MYSQL_QUERY_COLUMN_ERROR;
        }else if (e.contains(Constant.MYSQL_WHERE)){
            return DBUtilErrorCode.MYSQL_QUERY_SQL_ERROR;
        }
        return DBUtilErrorCode.READ_RECORD_FAIL;
    }

    public static DBUtilErrorCode oracleQueryErrorAna(String e){
        if (e.contains(Constant.ORACLE_TABLE_NAME)){
            return DBUtilErrorCode.ORACLE_QUERY_TABLE_NAME_ERROR;
        }else if (e.contains(Constant.ORACLE_SQL)){
            return DBUtilErrorCode.ORACLE_QUERY_SQL_ERROR;
        }else if (e.contains(Constant.ORACLE_INSERT_PRI)){
            return DBUtilErrorCode.ORACLE_QUERY_INSERT_PRI_ERROR;
        }
        return DBUtilErrorCode.READ_RECORD_FAIL;
    }

}
