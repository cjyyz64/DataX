package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSCriticalException;
import com.aliyun.openservices.ots.internal.model.Direction;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyType;
import com.aliyun.openservices.ots.internal.model.TableMeta;

public class Common {
    public static List<String> toColumnToGet(List<OTSColumn> columns, TableMeta meta) {
        Map<String, PrimaryKeyType> pk = meta.getPrimaryKeyMap();
        List<String> names = new ArrayList<String>();
        for (OTSColumn c : columns) {
            if (c.getColumnType() == OTSColumn.OTSColumnType.NORMAL && !pk.containsKey(c.getName())) {
                names.add(c.getName());
            }
        }
        return names;
    }
    
    public static Direction getDirection( List<PrimaryKeyColumn> begin, List<PrimaryKeyColumn> end) throws OTSCriticalException {
        int cmp = CompareHelper.comparePrimaryKeyColumnList(begin, end);
        if (cmp < 0) {
            return Direction.FORWARD;
        } else if (cmp > 0) {
            return Direction.BACKWARD;
        } else {
            throw new OTSCriticalException("Bug branch, the begin of range equals end of range.");
        }
    }
}
