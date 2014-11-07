package com.alibaba.datax.common.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 提供从 List<String> 中根据 regular 过滤的通用工具. 使用场景，比如：odpsreader
 * 的分区筛选，hdfsreader/txtfilereader的路径筛选等
 */
public final class FilterUtil {

    public static List<String> filterByRegular(List<String> allStrs,
                                               String regular) {
        List<String> matchedValues = new ArrayList<String>();

        // 语法习惯上的兼容处理(pt=* 实际正则应该是：pt=.*)
        String newReqular = regular.replace(".*", "*").replace("*", ".*");

        Pattern p = Pattern.compile(newReqular);

        for (String partition : allStrs) {
            if (p.matcher(partition).matches()) {
                matchedValues.add(partition);
            }
        }
        return matchedValues;
    }

    //已经去重
    public static List<String> filterByRegulars(List<String> allStrs,
                                                List<String> regulars) {
        Set<String> matchedValues = new HashSet<String>();

        List<String> tempMatched = null;
        for (String regular : regulars) {
            tempMatched = filterByRegular(allStrs, regular);
            if (null != tempMatched && !tempMatched.isEmpty()) {
                matchedValues.addAll(tempMatched);
            }
        }

        return new ArrayList<String>(matchedValues);
    }
}
