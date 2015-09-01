package com.alibaba.datax.common.statistics;

import com.alibaba.datax.common.util.HostUtils;
import com.google.common.base.Objects;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by liqiang on 15/8/23.
 */
public class PerfRecord implements Comparable<PerfRecord> {
    private static Logger perf = LoggerFactory.getLogger(PerfRecord.class);
    private static String datetimeFormat = "yyyy-MM-dd HH:mm:ss";


    public enum PHASE {
        /**
         * task total运行的时间，前10为框架统计，后面为部分插件的个性统计
         */
        TASK(0),

        READ_TASK_INIT(1),
        READ_TASK_PREPARE(2),
        READ_TASK_DATA(3),
        READ_TASK_POST(4),
        READ_TASK_DESTROY(5),

        WRITE_TASK_INIT(6),
        WRITE_TASK_PREPARE(7),
        WRITE_TASK_DATA(8),
        WRITE_TASK_POST(9),
        WRITE_TASK_DESTROY(10),

        /**
         * SQL_QUERY: sql query阶段, 部分reader的个性统计
         */
        SQL_QUERY(100),
        /**
         * 数据从sql全部读出来
         */
        RESULT_NEXT_ALL(101),

        /**
         * only odps block close
         */
        ODPS_BLOCK_CLOSE(102);

        private int val;

        PHASE(int val) {
            this.val = val;
        }

        public int toInt(){
            return val;
        }
    }

    private final int taskGroupId;
    private final int taskId;
    private final PHASE phase;
    private volatile String action;
    private volatile Date startTime;
    private volatile long elapsedTimeInNs = -1;
    private volatile long count = 0;
    private volatile long size = 0;

    private volatile long startTimeInNs;

    public PerfRecord(int taskGroupId, int taskId, PHASE phase) {
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
        this.phase = phase;
    }

    public static void addPerfRecord(int taskGroupId, int taskId, PHASE phase, long elapsedTimeInNs) {
        if(PerfTrace.getInstance().isEnable()) {
            PerfRecord perfRecord = new PerfRecord(taskGroupId, taskId, phase);
            perfRecord.elapsedTimeInNs = elapsedTimeInNs;
            perfRecord.action = "end";
            perfRecord.startTime = new Date();
            //在PerfTrace里注册
            PerfTrace.getInstance().tracePerfRecord(perfRecord);
            //perf.info(JSON.toJSONString(perfRecord));
            perf.info(perfRecord.toString());
        }
    }

    public void start() {
        if(PerfTrace.getInstance().isEnable()) {
            this.startTime = new Date();
            this.startTimeInNs = System.nanoTime();
            this.action = "start";
            //在PerfTrace里注册
            PerfTrace.getInstance().tracePerfRecord(this);
            //perf.info(JSON.toJSONString(this));
            perf.info(toString());
        }
    }

    public void addCount(long count) {
        this.count += count;
    }

    public void addSize(long size) {
        this.size += size;
    }

    public void end() {
        if(PerfTrace.getInstance().isEnable()) {
            this.elapsedTimeInNs = System.nanoTime() - startTimeInNs;
            this.action = "end";
            //perf.info(JSON.toJSONString(this));
            perf.info(toString());
        }
    }

    public void end(long elapsedTimeInNs) {
        if(PerfTrace.getInstance().isEnable()) {
            this.elapsedTimeInNs = elapsedTimeInNs;
            this.action = "end";
            //perf.info(JSON.toJSONString(this));
            perf.info(toString());
        }
    }

    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
                , getJobId(), taskGroupId, taskId, phase, action,
                DateFormatUtils.format(startTime, datetimeFormat), elapsedTimeInNs, count, size,getHostIP());
    }


    @Override
    public int compareTo(PerfRecord o) {
        if (o == null) {
            return 1;
        }
        return this.elapsedTimeInNs > o.elapsedTimeInNs ? 1 : this.elapsedTimeInNs == o.elapsedTimeInNs ? 0 : -1;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getJobId(),taskGroupId,taskId,phase,action,startTime,elapsedTimeInNs,count,size);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if(!(o instanceof PerfRecord)){
            return false;
        }

        PerfRecord dst = (PerfRecord)o;

        if(!Objects.equal(this.getJobId(),dst.getJobId())) return false;
        if(!Objects.equal(this.taskGroupId,dst.taskGroupId)) return false;
        if(!Objects.equal(this.taskId,dst.taskId)) return false;
        if(!Objects.equal(this.phase,dst.phase)) return false;
        if(!Objects.equal(this.action,dst.action)) return false;
        if(!Objects.equal(this.startTime,dst.startTime)) return false;
        if(!Objects.equal(this.elapsedTimeInNs,dst.elapsedTimeInNs)) return false;
        if(!Objects.equal(this.count,dst.count)) return false;
        if(!Objects.equal(this.size,dst.count)) return false;

        return true;
    }

    public PerfRecord copy() {
        PerfRecord copy = new PerfRecord(this.taskGroupId, this.getTaskId(), this.phase);
        copy.action = this.action;
        copy.startTime = this.startTime;
        copy.elapsedTimeInNs = this.elapsedTimeInNs;
        copy.count = this.count;
        copy.size = this.size;
        return copy;
    }
    public int getTaskGroupId() {
        return taskGroupId;
    }

    public int getTaskId() {
        return taskId;
    }

    public PHASE getPhase() {
        return phase;
    }

    public String getAction() {
        return action;
    }

    public long getElapsedTimeInNs() {
        return elapsedTimeInNs;
    }

    public long getCount() {
        return count;
    }

    public long getSize() {
        return size;
    }

    public long getJobId(){
        return PerfTrace.getInstance().getJobId();
    }

    public String getHostIP(){
       return HostUtils.IP;
    }

    public String getHostName(){
        return HostUtils.HOSTNAME;
    }

    public String getDatetime(){
        if(startTime == null){
            return "null time";
        }
        return DateFormatUtils.format(startTime, datetimeFormat);
    }
}
