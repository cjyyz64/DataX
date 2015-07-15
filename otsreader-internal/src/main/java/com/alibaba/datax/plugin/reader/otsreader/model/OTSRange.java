package com.alibaba.datax.plugin.reader.otsreader.model;

import java.util.List;

import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;

public class OTSRange {
    private List<PrimaryKeyColumn> begin = null;
    private List<PrimaryKeyColumn> end = null;
    private List<PrimaryKeyColumn> split = null;
    
    public List<PrimaryKeyColumn> getBegin() {
        return begin;
    }
    public void setBegin(List<PrimaryKeyColumn> begin) {
        this.begin = begin;
    }
    public List<PrimaryKeyColumn> getEnd() {
        return end;
    }
    public void setEnd(List<PrimaryKeyColumn> end) {
        this.end = end;
    }
    public List<PrimaryKeyColumn> getSplit() {
        return split;
    }
    public void setSplit(List<PrimaryKeyColumn> split) {
        this.split = split;
    }
}
