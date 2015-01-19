package com.alibaba.datax.core.container.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.CoreConstant;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public final class JobAssignUtil {

    /**
     * 公平的分配 task 到对应的 taskGroup 中。
     * 公平体现在：会考虑 task 中对资源负载作的 load 标识进行更均衡的作业分配操作。
     * TODO 具体文档举例说明
     */
    public final static List<Configuration> assignFairly(Configuration configuration, int channelNumber, int channelsPerTaskGroup) {
        Validate.isTrue(configuration != null, "框架获得的 Job 不能为 null.");

        List<Configuration> contentConfig = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        Validate.isTrue(contentConfig.size() > 0, "框架获得的切分后的 Job 无内容.");

        Validate.isTrue(channelNumber > 0 && channelsPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");

        int remainderChannelCount = channelNumber % channelsPerTaskGroup;
        int taskGroupNumber = channelNumber / channelsPerTaskGroup
                + (remainderChannelCount > 0 ? 1 : 0);

        Configuration aTaskConfig = contentConfig.get(0);

        String readerResourceMark = aTaskConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." +
                CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
        String writerResourceMark = aTaskConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." +
                CommonConstant.LOAD_BALANCE_RESOURCE_MARK);

        boolean hasLoadBalanceResourceMark = StringUtils.isNotBlank(readerResourceMark) ||
                StringUtils.isNotBlank(writerResourceMark);

        if (!hasLoadBalanceResourceMark) {
            // fake 一个固定的 key 作为资源标识（在 reader 或者 writer 上均可，此处选择在 reader 上进行 fake）
            for (Configuration conf : contentConfig) {
                conf.set(CoreConstant.JOB_READER_PARAMETER + "." +
                        CommonConstant.LOAD_BALANCE_RESOURCE_MARK, "aFakeResourceMarkForLoadBalance");
            }
        }

        LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap = parseAndGetResourceMarkAndTaskIdMap(contentConfig);
        List<Configuration> taskGroupConfig = doAssign(resourceMarkAndTaskIdMap, configuration, taskGroupNumber);
        // 调整 每个 tg 对应的 Channel 个数（属于优化范畴）
        adjustChannelNumPerTaskGroup(taskGroupConfig, channelNumber);
        return taskGroupConfig;
        //  return List<Configuration> taskGroupConfigs;
    }

    private static void adjustChannelNumPerTaskGroup(List<Configuration> taskGroupConfig, int channelNumber) {
        int taskGroupNumber = taskGroupConfig.size();
        int avgChannelsPerTaskGroup = channelNumber / taskGroupNumber;
        int remainderChannelCount = channelNumber % taskGroupNumber;
        // 表示有 remainderChannelCount 个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup + 1；
        // （taskGroupNumber - remainderChannelCount）个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup

        int i = 0;
        for (; i < remainderChannelCount; i++) {
            taskGroupConfig.get(i).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup + 1);
        }

        for (int j = 0; j < taskGroupNumber - remainderChannelCount; j++) {
            taskGroupConfig.get(i + j).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup);
        }
    }

    /**
     * 根据task 配置，获取到：
     * 资源名称 --> taskId(List) 的 map 映射关系
     */
    private static LinkedHashMap<String, List<Integer>> parseAndGetResourceMarkAndTaskIdMap(List<Configuration> contentConfig) {
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
            return readerResourceMarkAndTaskIdMap;
        } else {
            // 采用 writer 对资源做的标记进行 shuffle
            return writerResourceMarkAndTaskIdMap;
        }
    }


    /**
     * /**
     * 需要实现的效果通过例子来说是：
     * <pre>
     * a 库上有表：0, 1, 2
     * a 库上有表：3, 4
     * c 库上有表：5, 6, 7
     *
     * 则 shuffle 后的结果为：0, 3, 5, 1, 4, 6, 2, 7
     * </pre>
     */
    private static List<Configuration> doAssign(LinkedHashMap<String, List<Integer>> resourceMarkAndTaskIdMap,
                                                Configuration jobConfiguration, int taskGroupNumber) {
        List<Configuration> contentConfig = jobConfiguration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

        Configuration taskGroupTemplate = jobConfiguration.clone();
        taskGroupTemplate.remove(CoreConstant.DATAX_JOB_CONTENT);

        List<Configuration> result = new LinkedList<Configuration>();

        List<List<Configuration>> taskGroupConfigList = new LinkedList<List<Configuration>>();
        for (int i = 0; i < taskGroupNumber; i++) {
            taskGroupConfigList.add(new LinkedList<Configuration>());
        }

        int mapValueMaxLength = -1;

        List<String> resourceMarks = new ArrayList<String>();
        for (Map.Entry<String, List<Integer>> entry : resourceMarkAndTaskIdMap.entrySet()) {
            resourceMarks.add(entry.getKey());
            if (entry.getValue().size() > mapValueMaxLength) {
                mapValueMaxLength = entry.getValue().size();
            }
        }

        int taskGroupIndex = 0;
        for (int i = 0; i < mapValueMaxLength; i++) {
            for (int j = 0; j < resourceMarks.size(); j++) {
                if (resourceMarkAndTaskIdMap.get(resourceMarks.get(j)).size() > 0) {
                    int taskId = resourceMarkAndTaskIdMap.get(resourceMarks.get(j)).get(0);
                    taskGroupConfigList.get(taskGroupIndex % taskGroupNumber).add(contentConfig.get(taskId));
                    taskGroupIndex++;

                    resourceMarkAndTaskIdMap.get(resourceMarks.get(j)).remove(0);
                }
            }
        }

        Configuration tempTaskGroupConfig = null;
        for (int i = 0; i < taskGroupNumber; i++) {
            tempTaskGroupConfig = taskGroupTemplate.clone();
            tempTaskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupConfigList.get(i));
            tempTaskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);

            result.add(tempTaskGroupConfig);
        }

        return result;
    }

}
