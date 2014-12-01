package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.core.util.State;
import org.apache.commons.lang3.Validate;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jingxing on 14/11/8.
 *
 * 类比分布式情况下，slave的communication需要汇报到clusterManager上
 * 对于standalone和local模式下slave地communication也需要集中到一个地方
 * 用LocalSlaveCommunication来充当该两种模式下的communication集中地
 */
public class LocalSlaveContainerCommunication {
    private static Map<Integer, Communication>slaveContainerCommunicationMap =
            new ConcurrentHashMap<Integer, Communication>();

    public static void registerSlaveContainerCommunication(
            int slaveContainerId, Communication communication) {
        slaveContainerCommunicationMap.put(slaveContainerId, communication);
    }

    public static Communication getMasterCommunication() {
        Communication communication = new Communication();
        communication.setState(State.SUCCESS);

        for(Communication slaveContainerCommunication :
                slaveContainerCommunicationMap.values()) {
            communication.mergeFrom(slaveContainerCommunication);
        }

        return communication;
    }

    /**
     * 采用获取slaveContainerId后再获取对应communication的方式，
     * 防止map遍历时修改，同时也防止对map key-value对的修改
     * @return
     */
    public static Set<Integer> getSlaveContainerIdSet() {
        return slaveContainerCommunicationMap.keySet();
    }

    public static Communication getSlaveContainerCommunication(int slaveContainerId) {
        Validate.isTrue(slaveContainerId>=0, "slaveContainerId不能小于0");

        return slaveContainerCommunicationMap.get(slaveContainerId);
    }

    public static void updateSlaveContainerCommunication(final int slaveContainerId,
                                                         final Communication communication) {
        Validate.isTrue(slaveContainerCommunicationMap.containsKey(
                slaveContainerId), String.format("slaveContainerCommunicationMap中没有注册slaveContainerId[%d]的Communication，" +
                "无法更新该slaveContainer的信息", slaveContainerId));
        slaveContainerCommunicationMap.put(slaveContainerId, communication);
    }

    public static void clear() {
        slaveContainerCommunicationMap.clear();
    }

    public static Map<Integer, Communication> getSlaveContainerCommunicationMap() {
        return slaveContainerCommunicationMap;
    }
}
