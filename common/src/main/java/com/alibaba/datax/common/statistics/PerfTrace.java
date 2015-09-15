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
    private static final Object lock = new Object();
    private String perfTraceId;
    private volatile boolean enable;
    private volatile boolean isJob;
    private long jobId;
    private int priority;


    //jobid_jobversion,instanceid,taskid, src_mark, dst_mark,
    private Map<Integer, String> taskDetails = new ConcurrentHashMap<Integer, String>();
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

    public void addTaskDetails(int taskId, String detail) {
        if (enable) {
            String before = "";
            if (taskDetails.containsKey(taskId)) {
                before = taskDetails.get(taskId).trim();
            }
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
            //ArrayList非线程安全
            synchronized (this) {
                perfRecordMaps.get(perfRecord.getPhase()).add(perfRecord);
            }
        }
    }

    public String summarizeNoException(){
        String res;
        try {
            res = summarize();
        } catch (Exception e) {
            res = "PerfTrace summarize has Exception "+e.getMessage();
        }
        return res;
    }

    //任务结束时，对当前的perf总汇总统计
    private String summarize() {
        if (!enable) {
            return "PerfTrace not enable!";
        }

        StringBuilder info = new StringBuilder();
        info.append("\n === total summarize info === \n");
        info.append("\n   1. all phase average time info and max time task info: \n\n");
        info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %-100s\n", "PHASE", "AVERAGE USED TIME", "ALL TASK NUM", "MAX USED TIME", "MAX TASK ID", "MAX TASK INFO"));

        List<PerfRecord.PHASE> keys = new ArrayList<PerfRecord.PHASE>(perfRecordMaps.keySet());
        Collections.sort(keys, new Comparator<PerfRecord.PHASE>() {
            @Override
            public int compare(PerfRecord.PHASE o1, PerfRecord.PHASE o2) {
                return o1.toInt() - o2.toInt();
            }
        });
        for (PerfRecord.PHASE phase : keys) {
            List<PerfRecord> lists = perfRecordMaps.get(phase);
            if (lists == null) {
                continue;
            }
            long perfTimeTotal = 0;
            long averageTime = 0;
            long maxTime = 0;
            int maxTaskId = -1;
            int maxTaskGroupId = -1;
            for (PerfRecord perfRecord : lists) {
                if (perfRecord == null) {
                    LOG.info("phase(%s) has null PerfRecord", phase);
                    continue;
                }
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
            info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %-100s\n",
                    phase, unitTime(averageTime), lists.size(), unitTime(maxTime), jobId + "-" + maxTaskGroupId + "-" + maxTaskId, taskDetails.get(maxTaskId)));
        }

        List<PerfRecord> listCount = Optional.fromNullable(perfRecordMaps.get(PerfRecord.PHASE.READ_TASK_DATA)).or(new ArrayList<PerfRecord>());

        long recordsTotal = 0;
        long sizesTotal = 0;
        long averageRecords = 0;
        long averageBytes = 0;
        long maxRecord = 0;
        long maxByte = 0;
        int maxTaskId4Records = -1;
        int maxTGID4Records = -1;
        for (PerfRecord perfRecord : listCount) {
            recordsTotal += perfRecord.getCount();
            sizesTotal += perfRecord.getSize();
            if (perfRecord.getCount() > maxRecord) {
                maxRecord = perfRecord.getCount();
                maxByte = perfRecord.getSize();
                maxTaskId4Records = perfRecord.getTaskId();
                maxTGID4Records = perfRecord.getTaskGroupId();
            }
        }
        if (listCount.size() > 0) {
            averageRecords = recordsTotal / listCount.size();
            averageBytes = recordsTotal / listCount.size();
        }
        //Min min = new Min();

        info.append("\n\n 2. record average count and max count task info :\n\n");
        info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %18s | %-100s\n", "PHASE", "AVERAGE RECORDS", "AVERAGE BYTES", "MAX RECORDS", "MAX RECORD`S BYTES", "MAX TASK ID", "MAX TASK INFO"));
        if (maxTaskId4Records > -1) {
            info.append(String.format("%-20s | %18s | %18s | %18s | %18s | %18s | %-100s\n"
                    , PerfRecord.PHASE.READ_TASK_DATA, averageRecords, unitSize(averageBytes), maxRecord, unitSize(maxByte), jobId + "-" + maxTGID4Records + "-" + maxTaskId4Records, taskDetails.get(maxTaskId4Records)));

        }
        return info.toString();
    }

    //缺省传入的时间是nano
    public static String unitTime(long time) {
        return unitTime(time, TimeUnit.NANOSECONDS);
    }

    public static String unitTime(long time, TimeUnit timeUnit) {
        return String.format("%,.3fs", ((float) timeUnit.toNanos(time)) / 1000000000);
    }

    public static String unitSize(long size) {
        if (size > 1000000000) {
            return String.format("%,.2fG", (float) size / 1000000000);
        } else if (size > 1000000) {
            return String.format("%,.2fM", (float) size / 1000000);
        } else if (size > 1000) {
            return String.format("%,.2fK", (float) size / 1000);
        } else {
            return size + "B";
        }
    }


    public ConcurrentHashMap<PerfRecord.PHASE, List<PerfRecord>> getPerfRecordMaps() {
        return perfRecordMaps;
    }

    public Map<Integer, String> getTaskDetails() {
        return taskDetails;
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
