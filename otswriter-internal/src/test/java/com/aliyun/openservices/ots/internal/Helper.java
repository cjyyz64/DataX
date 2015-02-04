package com.aliyun.openservices.ots.internal;

import java.util.List;
import java.util.regex.Pattern;

import com.aliyun.openservices.ots.internal.OTSErrorCode;
import com.aliyun.openservices.ots.internal.OTSException;
import com.aliyun.openservices.ots.internal.model.Column;
import com.aliyun.openservices.ots.internal.model.PrimaryKey;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;

public class Helper {
    
    public static long getPKSize(PrimaryKey pks) {
        int rowSize = 0;
        for(PrimaryKeyColumn pk : pks.getPrimaryKeyColumns()) {
            rowSize += pk.getName().length();
            switch (pk.getValue().getType()) {
                case INTEGER:
                    rowSize += 8;
                    break;
                case STRING:
                    rowSize += pk.getValue().asString().length();
                    break;
                default:
                    break;
            }
        }
        return rowSize;
    }
    
    public static long getAttrSize(List<Column> attrs) throws OTSException {
        int rowSize = 0;
        for (Column attr : attrs) {
            //检查列名
            if (!Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*").matcher(attr.getName()).matches()) {
                throw new OTSException(
                        "Column name invalid", 
                        null, 
                        OTSErrorCode.INVALID_PARAMETER, 
                        "RequestId",
                        400);
            }
            rowSize += attr.getName().length();
            if (attr.getValue() != null) {
                switch (attr.getValue().getType()) {
                    case BINARY:
                        rowSize += attr.getValue().asBinary().length;
                        break;
                    case BOOLEAN:
                        rowSize += 1;
                        break;
                    case DOUBLE:
                        rowSize += 8;
                        break;
                    case INTEGER:
                        rowSize += 8;
                        break;
                    case STRING:
                        if (attr.getValue().asString() != null) {
                            rowSize += attr.getValue().asString().length();
                        }
                        break;
                    default:
                        break;
                        }
            }
        }
        return rowSize;
    }
    
    public static long getRowSize(PrimaryKey pk, List<Column> attr) throws OTSException {
        return getPKSize(pk) + getAttrSize(attr);
    }
    
    public static long getCU(PrimaryKey pk, List<Column> attr) throws OTSException {
        long rowSize = getRowSize(pk, attr);
        long expectCU = rowSize % 1024 > 0 ? rowSize / 1024 + 1 : rowSize / 1024;
        return expectCU;
    }
}
