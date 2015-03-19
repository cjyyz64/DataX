package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.List;

public class Tablet implements Serializable{

    public final RowkeyMeta meta;
    public final String startkey;
    public final String endkey;
    public static final String MIN = "min";
    public static final String MAX = "max";

    public Tablet(RowkeyMeta meta,String startkey, String endkey){
        this.meta = meta;
        this.startkey = startkey;
        this.endkey = endkey;
    }

    public Tablet(RowkeyMeta meta, List<String> startkey, List<String> endkey) {
        this.meta = meta;
        this.startkey = this.vector(startkey);
        this.endkey = this.vector(endkey);
    }

    private String vector(List<String> rowkey) {
        Preconditions.checkArgument(rowkey != null && !rowkey.isEmpty(), "expect rowkey not-null");
        if (rowkey.get(0).contains("__OB__MIN__")) return MIN;
        if (rowkey.get(0).contains("__OB__MAX__")) return MAX;
        StringBuilder builder = new StringBuilder("(");
        for (RowkeyMeta.Entry entry : meta) {
            String value = rowkey.get(entry.position - 1);
            builder.append(entry.convert(value)).append(",");
        }
        builder.append(")");
        return builder.deleteCharAt(builder.lastIndexOf(",")).toString();
    }

    private String vector() {
        StringBuilder builder = new StringBuilder("(");
        for (RowkeyMeta.Entry entry : meta) {
            builder.append(entry.name).append(",");
        }
        builder.append(")");
        return builder.deleteCharAt(builder.lastIndexOf(",")).toString();
    }

    public String sql(SelectExpression select, String boundRowkey, int limit) {
        String sql = select.toSQL();
        String keyword = select.where == null ? "where" : "and";
        boolean min = boundRowkey == null && this.startkey.equals(MIN);
        boolean max = this.endkey.equals(MAX);
        if (min && max) {
            return String.format("%s limit %s", sql, limit);
        } else if (min) {
            return String.format("%s %s %s <= %s limit %s", sql, keyword, vector(), endkey, limit);
        } else if (max) {
            if(boundRowkey == null){
                return String.format("%s %s %s >= %s limit %s", sql, keyword, vector(), startkey, limit);
            }else {
                return String.format("%s %s %s > %s limit %s", sql, keyword, vector(), boundRowkey, limit);
            }
        } else {
            return String.format("%s %s (%s > %s and %s <= %s) limit %s", sql, keyword, vector(), boundRowkey == null ? startkey : boundRowkey, vector(), endkey, limit);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof Tablet) {
            return Objects.equal(startkey, (((Tablet) obj).startkey)) && Objects.equal(endkey, (((Tablet) obj).endkey));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(meta, startkey, endkey);
    }

}
