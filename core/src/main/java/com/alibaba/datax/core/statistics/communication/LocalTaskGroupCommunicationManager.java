package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.dataxservice.face.domain.State;
import org.apache.commons.lang3.Validate;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * 类比分布式情况下，taskGroup的communication需要汇报到 DataXService 上
 * 对于 standalone 和 local 模式下 taskGroup 的 communication 也需要集中到一个地方
 * 用 LocalTaskGroupCommunication 来充当该两种模式下的communication集中地
 */
public class LocalTaskGroupCommunicationManager {
    private static Map<Integer, Communication> taskGroupCommunicationMap =
            new ConcurrentHashMap<Integer, Communication>();

    public static void registerTaskGroupCommunication(
            int taskGroupId, Communication communication) {
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

    public static Communication getJobCommunication() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for(Communication taskGroupCommunication :
                taskGroupCommunicationMap.values()) {
            communication.mergeFrom(taskGroupCommunication);
        }

        return communication;
    }

    /**
     * 采用获取taskGroupId后再获取对应communication的方式，
     * 防止map遍历时修改，同时也防止对map key-value对的修改
     * @return
     */
    public static Set<Integer> getTaskGroupIdSet() {
        return taskGroupCommunicationMap.keySet();
    }

    public static Communication getTaskGroupCommunication(int taskGroupId) {
        Validate.isTrue(taskGroupId>=0, "taskGroupId不能小于0");

        return taskGroupCommunicationMap.get(taskGroupId);
    }

    public static void updateTaskGroupCommunication(final int taskGroupId,
                                                    final Communication communication) {
        Validate.isTrue(taskGroupCommunicationMap.containsKey(
                taskGroupId), String.format("taskGroupCommunicationMap中没有注册taskGroupId[%d]的Communication，" +
                "无法更新该taskGroup的信息", taskGroupId));
        taskGroupCommunicationMap.put(taskGroupId, communication);
    }

    public static void clear() {
        taskGroupCommunicationMap.clear();
    }

    public static Map<Integer, Communication> getTaskGroupCommunicationMap() {
        return taskGroupCommunicationMap;
    }
}
