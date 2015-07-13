package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.List;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.alibaba.datax.plugin.reader.otsreader.utils.CompareHelper;
import com.google.gson.Gson;

import static org.junit.Assert.*;

public class AssertHelper {
    
    public static void assertOTSRange(OTSRange src, OTSRange target) {
        if ((src == null && target != null) || (src != null && target == null)) {
            fail();
        }
        if (src != null && target != null) {
            if ((src.getBegin() == null && target.getBegin() != null) || (src.getBegin() != null && target.getBegin() == null)) {
                fail();
            }
            if ((src.getEnd() == null && target.getEnd() != null) || (src.getEnd() != null && target.getEnd() == null)) {
                fail();
            }
            if ((src.getSplit() == null && target.getSplit() != null) || (src.getSplit() != null && target.getSplit() == null)) {
                fail();
            }
            if (((src.getBegin() != null && target.getBegin() != null) && CompareHelper.comparePrimaryKeyColumnList(src.getBegin(), target.getBegin()) != 0) ) {
                fail();
            }
            if (((src.getEnd() != null && target.getEnd() != null) && CompareHelper.comparePrimaryKeyColumnList(src.getEnd(), target.getEnd()) != 0) ) {
                fail();
            }
            if (((src.getSplit() != null && target.getSplit() != null) && CompareHelper.comparePrimaryKeyColumnListList(src.getSplit(), target.getSplit()) != 0) ) {
                fail();
            }
            //            if (
            //                    (src.getBegin() == null && target.getBegin() != null) || (src.getBegin() != null && target.getBegin() == null) ||
            //                    (src.getEnd() == null && target.getEnd() != null) || (src.getEnd() != null && target.getEnd() == null) ||
            //                    (src.getSplit() == null && target.getSplit() != null) || (src.getSplit() != null && target.getSplit() == null)
            //               ) {
            //                fail();
            //            } 
            //            else {
            //                if (
            //                        ((src.getBegin() != null && target.getBegin() != null) && CompareHelper.comparePrimaryKeyColumnList(src.getBegin(), target.getBegin()) != 0) ||
            //                        ((src.getEnd() != null && target.getEnd() != null) && CompareHelper.comparePrimaryKeyColumnList(src.getEnd(), target.getEnd()) != 0) ||
            //                        ((src.getSplit() != null && target.getSplit() != null) && CompareHelper.comparePrimaryKeyColumnListList(src.getSplit(), target.getSplit()) != 0) 
            //                        ){
            //                    fail();
            //                }
            //            }
        }
    }
    
    public static void assertOTSColumn(List<OTSColumn> src, List<OTSColumn> target) {
        if ((src == null && target != null) || (src != null && target == null)) {
            fail();
        }
        Gson g = new Gson();
        if (src != null && target != null) {
            assertEquals(src.size(), target.size());
            for (int i = 0; i < src.size(); i++) {
                OTSColumn s = src.get(i);
                OTSColumn t = target.get(i);
                
                assertEquals(s.getName(), t.getName());
                assertEquals(s.getColumnType(), t.getColumnType());
                
                assertEquals(g.toJson(s.getValue()), g.toJson(t.getValue()));
            }
        }
    }
}
