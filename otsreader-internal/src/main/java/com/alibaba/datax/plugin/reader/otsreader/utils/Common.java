package com.alibaba.datax.plugin.reader.otsreader.utils;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;

public class Common {
    public static List<String> toColumnToGet(List<OTSColumn> columns) {
        List<String> names = new ArrayList<String>();
        for (OTSColumn c : columns) {
            if (c.getColumnType() == OTSColumn.OTSColumnType.NORMAL) {
                names.add(c.getName());
            }
        }
        return names;
    }
}
