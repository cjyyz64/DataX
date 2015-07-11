package com.alibaba.datax.plugin.reader.otsreader.model;

import java.util.List;

import com.alibaba.datax.plugin.reader.otsreader.utils.CompareHelper;
import com.aliyun.openservices.ots.internal.model.PrimaryKeyColumn;

public class OTSRange {
    private List<PrimaryKeyColumn> begin = null;
    private List<PrimaryKeyColumn> end = null;
    private List<List<PrimaryKeyColumn>> split = null;
    
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
    public List<List<PrimaryKeyColumn>> getSplit() {
        return split;
    }
    public void setSplit(List<List<PrimaryKeyColumn>> split) {
        this.split = split;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof OTSRange)) {
            return false;
        }
        OTSRange other = (OTSRange) o;
        
        if (
                (this.begin == null && other.begin != null) || (this.begin != null && other.begin == null) ||
                (this.end == null && other.end != null) || (this.end != null && other.end == null) ||
                (this.split == null && other.split != null) || (this.split != null && other.split == null)
           ) {// 其中一个属性为空，但是另外一个对象的属性不为空
            return false;
        } else {
            if (
                    ((this.begin != null && other.begin != null) && CompareHelper.comparePrimaryKeyColumnList(this.begin, other.begin) != 0) ||
                    ((this.end != null && other.end != null) && CompareHelper.comparePrimaryKeyColumnList(this.end, other.end) != 0) ||
                    ((this.split != null && other.split != null) && CompareHelper.comparePrimaryKeyColumnListList(this.split, other.split) != 0) 
                    ){
                return false;
            }
        }
        return true;
    }
}
