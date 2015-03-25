package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Tablet implements Serializable{

    public final RowkeyMeta meta;
    public final List<?> startkey;
    public final List<?> endkey;

    public Tablet(RowkeyMeta meta, List<?> startkey, List<?> endkey) {
        this.meta = meta;
        this.startkey = startkey;
        this.endkey = endkey;
    }

    private String vector() {
        StringBuilder builder = new StringBuilder("(");
        for (RowkeyMeta.Entry entry : meta) {
            builder.append(entry.name).append(",");
        }
        builder.append(")");
        return builder.deleteCharAt(builder.lastIndexOf(",")).toString();
    }

    private String placeholder() {
        StringBuilder builder = new StringBuilder("(");
        for (RowkeyMeta.Entry entry : meta) {
            builder.append("?").append(",");
        }
        builder.append(")");
        return builder.deleteCharAt(builder.lastIndexOf(",")).toString();
    }

    public String sql(SelectExpression select, List<?> boundRowkey, int limit) {
        String sql = select.toSQL();
        String keyword = select.where == null ? "where" : "and";
        boolean min = boundRowkey.isEmpty() && RowkeyMeta.BoundValue.isMin(this.startkey.get(0));
        boolean max = RowkeyMeta.BoundValue.isMax(this.endkey.get(0));
        if (min && max) {
            return String.format("%s limit %s", sql, limit);
        } else if (min) {
            return String.format("%s %s %s <= %s limit %s", sql, keyword, vector(), placeholder(), limit);
        } else if (max) {
            if(boundRowkey.isEmpty()){
                return String.format("%s %s %s >= %s limit %s", sql, keyword, vector(), placeholder(), limit);
            }else {
                return String.format("%s %s %s > %s limit %s", sql, keyword, vector(), placeholder(), limit);
            }
        } else {
            return String.format("%s %s (%s > %s and %s <= %s) limit %s", sql, keyword, vector(), placeholder(), vector(), placeholder(), limit);
        }
    }

    public List<?> parameters(List<?> boundRowkey) {
        class Helper{
            private List<?> parameters(List<?> a, List<?> b){
                List<?> parameters = Lists.newArrayListWithCapacity(a.size() + b.size());
                parameters.addAll((Collection)a);
                parameters.addAll((Collection)b);
                return parameters;
            }
        }
        boolean min = RowkeyMeta.BoundValue.isMin(this.startkey.get(0));
        boolean max = RowkeyMeta.BoundValue.isMax(this.endkey.get(0));
        if(boundRowkey.isEmpty()){
            if (min && max) {
                return Collections.emptyList();
            } else if (min) {
                return endkey;
            } else if (max) {
                return startkey;
            } else {
                return new Helper().parameters(startkey,endkey);
            }
        }else {
            if (min && max) {
                return boundRowkey;
            } else if (min) {
                return new Helper().parameters(boundRowkey,endkey);
            } else if (max) {
                return boundRowkey;
            } else {
                return new Helper().parameters(boundRowkey,endkey);
            }
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
