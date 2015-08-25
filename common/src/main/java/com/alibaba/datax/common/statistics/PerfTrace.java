package com.alibaba.datax.common.statistics;

import com.google.common.base.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * PerfTrace 记录 job（local模式），taskGroup（distribute模式），因为这2种都是jvm，即一个jvm里只需要有1个PerfTrace。
 */

public class PerfTrace {

    private static Logger LOG = LoggerFactory.getLogger(PerfTrace.class);
    private static PerfTrace instance;
    private static Object lock = new Object();
    private String perfTraceId;
    private volatile boolean enable;
    private volatile boolean isJob;
    private long jobId;
    private int priority;


    //jobid_jobversion,instanceid,taskid, src_mark, dst_mark,
    private Map<Long, String> taskDetails = new ConcurrentHashMap<Long, String>();
    //PHASE => PerfRecord
    private ConcurrentHashMap<PerfRecord.PHASE, List<PerfRecord>> perfRecordMaps = new ConcurrentHashMap<PerfRecord.PHASE, List<PerfRecord>>();

    /**
     * 单实例
     *
     * @param isJob
     * @param jobId
     * @param taskGroupId
     * @return
     */
    public static PerfTrace getInstance(boolean isJob, long jobId, int taskGroupId,int priority, boolean enable) {

        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new PerfTrace(isJob, jobId, taskGroupId,priority, enable);
                }
            }
        }
        return instance;
    }

    /**
     * 因为一个JVM只有一个，因此在getInstance(isJob,jobId,taskGroupId)调用完成实例化后，方便后续调用，直接返回该实例
     *
     * @return
     */
    public static PerfTrace getInstance() {
        if (instance == null) {
            LOG.error("PerfTrace instance not be init! must have some error! ");
            synchronized (lock) {
                if (instance == null) {
                    instance = new PerfTrace(false, -1111, -1111, 0, false);
                }
            }
        }
        return instance;
    }

    private PerfTrace(boolean isJob, long jobId, int taskGroupId, int priority, boolean enable) {
        this.perfTraceId = isJob ? "job_" + jobId : String.format("taskGroup_%s_%s", jobId, taskGroupId);
        this.enable = enable;
        this.isJob = isJob;
        this.jobId = jobId;
        this.priority = priority;
        LOG.info(String.format("PerfTrace traceId=%s, isEnable=%s, priority=%s", this.perfTraceId, this.enable, this.priority));

    }

    public void addTaskDetails(long taskId, String detail) {
        if (enable) {

            String before = taskDetails.get(taskId);
            if (StringUtils.isEmpty(before)) {
                before = "";
            } else {
                before += ",";
            }

            this.taskDetails.put(taskId, before + detail);
        }
    }

    public void tracePerfRecord(PerfRecord perfRecord) {
        if (enable) {
            perfRecordMaps.putIfAbsent(perfRecord.getPhase(), new ArrayList<PerfRecord>());
            perfRecordMaps.get(perfRecord.getPhase()).add(perfRecord);
        }
    }

    //任务结束时，对当前的perf总汇总统计
    public String summarize() {
        if (!enable) {
            return "PerfTrace not enable!";
        }

        StringBuilder info = new StringBuilder();
        info.append("\n\n === total summarize info === \n");
        info.append("\n\n used time info =>\n\n");
        info.append(String.format("%-20s | %18s | %10s | %15s | %18s | %-100s\n", "PHASE", "AVERAGE USED TIME", "TASK NUM", "MAX USED TIME", "MAX TASK ID", "MAX TASK INFO"));

        List<PerfRecord.PHASE> keys = new ArrayList<PerfRecord.PHASE>(perfRecordMaps.keySet());
        Collections.sort(keys, new Comparator<PerfRecord.PHASE>() {
            @Override
            public int compare(PerfRecord.PHASE o1, PerfRecord.PHASE o2) {
                return o1.toInt() - o2.toInt();
            }
        });
        for (PerfRecord.PHASE phase : keys) {
            List<PerfRecord> lists = perfRecordMaps.get(phase);
            long perfTimeTotal = 0;
            long averageTime = 0;
            long maxTime = 0;
            long maxTaskId = -1;
            int maxTaskGroupId = -1;
            for (PerfRecord perfRecord : lists) {
                perfTimeTotal += perfRecord.getElapsedTimeInNs();
                if (perfRecord.getElapsedTimeInNs() > maxTime) {
                    maxTime = perfRecord.getElapsedTimeInNs();
                    maxTaskId = perfRecord.getTaskId();
                    maxTaskGroupId = perfRecord.getTaskGroupId();
                }
            }

            if (lists.size() > 0) {
                averageTime = perfTimeTotal / lists.size();
            }
            info.append(String.format("%-20s | %18s | %10s | %15s | %18s | %-100s\n",
                    phase, unitTime(averageTime), lists.size(), unitTime(maxTime), jobId+"-"+maxTaskGroupId+"-"+maxTaskId, taskDetails.get(maxTaskId)));
        }

        List<PerfRecord> listCount = Optional.fromNullable(perfRecordMaps.get(PerfRecord.PHASE.READ_TASK_DATA)).or(new ArrayList<PerfRecord>());

        long recordsTotal = 0;
        long averageCount = 0;
        long maxCount = 0;
        long maxTaskId4Count = -1;
        int maxTGID4Count = -1;
        for (PerfRecord perfRecord : listCount) {
            recordsTotal += perfRecord.getCount();
            if (perfRecord.getCount() > maxCount) {
                maxCount = perfRecord.getCount();
                maxTaskId4Count = perfRecord.getTaskId();
                maxTGID4Count = perfRecord.getTaskGroupId();
            }
        }
        if (listCount.size() > 0) {
            averageCount = recordsTotal / listCount.size();
        }
        //Min min = new Min();

        info.append("\n\n max count info =>\n\n");
        info.append(String.format("%-20s | %18s | %10s | %15s | %18s | %-100s\n", "PHASE", "AVERAGE COUNT", "TASK NUM", "MAX COUNT", "MAX TASK ID", "MAX TASK INFO"));
        if(maxTaskId4Count>-1) {
            info.append(String.format("%-20s | %18s | %10s | %15s | %18s | %-100s\n"
                    , PerfRecord.PHASE.READ_TASK_DATA, averageCount, listCount.size(), maxCount, jobId + "-" + maxTGID4Count + "-" + maxTaskId4Count, taskDetails.get(maxTaskId4Count)));

        }
        return info.toString();
    }

    public String unitTime(long time) {
        //100s以上用s
        if (time > 100000000000L) {
            return TimeUnit.NANOSECONDS.toSeconds(time) + "s";
        } else if (time > 100000000L) {
            //100ms以上用ms
            return TimeUnit.NANOSECONDS.toMillis(time) + "ms";
        } else if (time > 100000L) {
            //100us以上用us
            return TimeUnit.NANOSECONDS.toMicros(time) + "us";
        } else {
            return time + "ns";
        }
    }


    public boolean isEnable() {
        return enable;
    }

    public boolean isJob() {
        return isJob;
    }

    public long getJobId() {
        return jobId;
    }
}
