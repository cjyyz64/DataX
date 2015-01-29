package com.alibaba.datax.plugin.writer.adswriter.odps;

import java.util.Iterator;
import java.util.List;

/**
 * ODPS table meta.
 *
 * @author <a href="mailto:yujun.lyj@alibaba-inc.com">xiaobai</a>
 * @since 0.0.1
 */
public class TableMeta {

    private String tableName;

    private List<FieldSchema> cols;

    private List<FieldSchema> partitionKeys;

    private int lifeCycle;

    private String comment;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<FieldSchema> getCols() {
        return cols;
    }

    public void setCols(List<FieldSchema> cols) {
        this.cols = cols;
    }

    public List<FieldSchema> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<FieldSchema> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public int getLifeCycle() {
        return lifeCycle;
    }

    public void setLifeCycle(int lifeCycle) {
        this.lifeCycle = lifeCycle;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String toString() {
        return toDDL();
    }

    /**
     * @return <br>
     *         "CREATE TABLE [IF NOT EXISTS] table_name <br>
     *         [(col_name data_type [COMMENT col_comment], ...)] <br>
     *         [COMMENT table_comment] <br>
     *         [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)] <br>
     *         [LIFECYCLE days] <br>
     *         [AS select_statement] " <br>
     */
    public String toDDL() {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE " + tableName).append(" ");
        List<FieldSchema> cols = this.cols;
        if (cols != null && cols.size() > 0) {
            builder.append("(").append(toDDL(cols)).append(")").append(" ");
        }
        List<FieldSchema> partitionKeys = this.partitionKeys;
        if (partitionKeys != null && partitionKeys.size() > 0) {
            builder.append("PARTITIONED BY ");
            builder.append("(").append(toDDL(partitionKeys)).append(")").append(" ");
        }
        if (lifeCycle > 0) {
            builder.append("LIFECYCLE " + lifeCycle).append(" ");
        }
        return builder.toString();
    }

    private String toDDL(List<FieldSchema> cols) {
        StringBuilder builder = new StringBuilder();
        Iterator<FieldSchema> iter = cols.iterator();
        builder.append(iter.next());
        while (iter.hasNext()) {
            builder.append(",").append(iter.next().toDDL());
        }
        return builder.toString();
    }

}
