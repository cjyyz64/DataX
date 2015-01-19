package com.alibaba.datax.core.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public final class JobShuffleUtil {

    /**
     * 参数 contentConfig 是 reader/writer split 后的 task 一一对应后的 task 配置。
     * 此处根据配置中是否存在 load 标识对 task 进行更均衡的 shuffle 操作。
     */
    public final static List<Configuration> shuffleForLoadBalance(List<Configuration> contentConfig) {
        if (contentConfig == null || contentConfig.isEmpty()) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR, "框架获得的切分后的Job 无内容.");
        }

        if (contentConfig.size() == 1) {
            // 没必要 shuffle
            return contentConfig;
        }

        Configuration aTaskConfig = contentConfig.get(0);
        String readerResourceMark = Configuration.from(aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER)).getString(CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
        String writerResourceMark = Configuration.from(aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER)).getString(CommonConstant.LOAD_BALANCE_RESOURCE_MARK);

        boolean hasLoadBalanceResourceMark = StringUtils.isNotBlank(readerResourceMark) || StringUtils.isNotBlank(writerResourceMark);

        if (hasLoadBalanceResourceMark) {
            doShuffleByResourceMark(contentConfig);
        } else {
            Collections.shuffle(contentConfig,
                    new Random(System.currentTimeMillis()));
        }
        return contentConfig;
    }

    private static List<Configuration> doShuffleByResourceMark(List<Configuration> contentConfig) {
        // key: resourceMark, value: taskId
        LinkedHashMap<String, List<Integer>> readerResourceMarkAndTaskIdMap = new LinkedHashMap<String, List<Integer>>();
        LinkedHashMap<String, List<Integer>> writerResourceMarkAndTaskIdMap = new LinkedHashMap<String, List<Integer>>();

        String readerResourceMark = null;
        String writerResourceMark = null;
        Configuration aTaskConfig = null;
        for (int i = 0, len = contentConfig.size(); i < len; i++) {
            aTaskConfig = contentConfig.get(i);

            // 把 readerResourceMark 加到 readerResourceMarkAndTaskIdMap 中
            readerResourceMark = Configuration.from(aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER)).getString(CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            if (readerResourceMarkAndTaskIdMap.get(readerResourceMark) == null) {
                readerResourceMarkAndTaskIdMap.put(readerResourceMark, new LinkedList<Integer>());
            }
            readerResourceMarkAndTaskIdMap.get(readerResourceMark).add(aTaskConfig.getInt(CoreConstant.JOB_TASK_ID));

            // 把 writerResourceMark 加到 writerResourceMarkAndTaskIdMap 中
            writerResourceMark = Configuration.from(aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER)).getString(CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            if (writerResourceMarkAndTaskIdMap.get(writerResourceMark) == null) {
                writerResourceMarkAndTaskIdMap.put(writerResourceMark, new LinkedList<Integer>());
            }
            writerResourceMarkAndTaskIdMap.get(writerResourceMark).add(aTaskConfig.getInt(CoreConstant.JOB_TASK_ID));
        }

        if (readerResourceMarkAndTaskIdMap.size() >= writerResourceMarkAndTaskIdMap.size()) {
            // 采用 reader 对资源做的标记进行 shuffle
            return doShuffle(readerResourceMarkAndTaskIdMap, contentConfig);
        } else {
            // 采用 writer 对资源做的标记进行 shuffle
            return doShuffle(writerResourceMarkAndTaskIdMap, contentConfig);
        }
    }

    /**
     * 需要实现的效果通过例子来说是：
     * <pre>
     * a 库上有表：0, 1, 2
     * a 库上有表：3, 4
     * c 库上有表：5, 6, 7
     *
     * 则 shuffle 后的结果为：0, 3, 5, 1, 4, 6, 2, 7
     * </pre>
     *
     */
    private static List<Configuration> doShuffle(LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap, List<Configuration> contentConfig) {
        int mapValueMaxLength = -1;

        List<Configuration> result = new LinkedList<Configuration>();

        List<String> resourceMarks = new ArrayList<String>();
        for (Map.Entry<String, List<Integer>> entry : resourceMarkAndTaskIdMap.entrySet()) {
            resourceMarks.add(entry.getKey());
            if (entry.getValue().size() > mapValueMaxLength) {
                mapValueMaxLength = entry.getValue().size();
            }
        }

        for (int i = 0; i < mapValueMaxLength; i++) {
            for (int j = 0; j < resourceMarks.size(); j++) {
                if (resourceMarkAndTaskIdMap.get(resourceMarks.get(j)).size() > 0) {
                    int taskId = resourceMarkAndTaskIdMap.get(resourceMarks.get(j)).get(0);
                    result.add(contentConfig.get(taskId));
                    resourceMarkAndTaskIdMap.get(resourceMarks.get(j)).remove(0);
                }
            }
        }

        return result;
    }

}
