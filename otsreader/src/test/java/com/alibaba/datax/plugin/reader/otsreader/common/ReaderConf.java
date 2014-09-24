package com.alibaba.datax.plugin.reader.otsreader.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.alibaba.datax.plugin.reader.otsreader.Key;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSColumn;
import com.alibaba.datax.plugin.reader.otsreader.model.OTSConf;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;

public class ReaderConf {
    
    private OTSConf conf;
    
    public OTSConf getConf() {
        return conf;
    }

    public void setConf(OTSConf conf) {
        this.conf = conf;
    }
    
    private String linesToString(List<String> lines) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < lines.size(); i++) {
            if (i < lines.size() -1) {
                sb.append(lines.get(i));
                sb.append(",\n");
            } else {
                sb.append(lines.get(i));
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    private String columnToString(List<OTSColumn> columns) {
        List<String> lines = new ArrayList<String>();
        for (OTSColumn col : columns) {
            if (col.getType() == OTSColumn.OTSColumnType.NORMAL) {
                lines.add(String.format("\t\t\"%s\"", col.getValue().asString()));
            } else {
                if (col.getValue().getType() == ColumnType.STRING) {
                    lines.add(String.format("\t\t{ \"type\":\"STRING\", \"value\":\"%s\" }", col.getValue().asString()));
                } else if (col.getValue().getType() == ColumnType.INTEGER) {
                    lines.add(String.format("\t\t{ \"type\":\"INTEGER\", \"value\":\"%d\" }", col.getValue().asLong()));
                } else if (col.getValue().getType() == ColumnType.DOUBLE) {
                    lines.add(String.format("\t\t{ \"type\":\"DOUBLE\", \"value\":\"%f\" }", col.getValue().asDouble()));
                } else if (col.getValue().getType() == ColumnType.BOOLEAN) {
                    lines.add(String.format("\t\t{ \"type\":\"BOOLEAN\", \"value\":\"%s\" }", col.getValue().asBoolean()));
                } else if (col.getValue().getType() == ColumnType.BINARY) {
                    lines.add(String.format("\t\t{ \"type\":\"BINARY\", \"value\":\"%s\" }", Base64.encodeBase64String(col.getValue().asBinary())));
                }
            }
        }
        return linesToString(lines);
    }
    
    private String rangeToString(List<PrimaryKeyValue> columns) {
        List<String> lines = new ArrayList<String>();
        for (PrimaryKeyValue col: columns) {
            if (col == PrimaryKeyValue.INF_MAX) {
                lines.add("\t\t{\"type\":\"INF_MAX\", \"value\":\"\"}");
            } else if (col == PrimaryKeyValue.INF_MIN) {
                lines.add("\t\t{\"type\":\"INF_MIN\", \"value\":\"\"}");
            } else {
                if (col.getType() == PrimaryKeyType.INTEGER) {
                    lines.add(String.format("\t\t{\"type\":\"INTEGER\", \"value\":\"%d\"}", col.asLong()));
                } else {
                    lines.add(String.format("\t\t{\"type\":\"STRING\", \"value\":\"%s\"}", col.asString()));
                }
            }
        }
        return linesToString(lines);
    }

    public String toString() {
        List<String> lines = new ArrayList<String>();
        if (conf.getEndpoint() != null) {
            lines.add(String.format("\t\"%s\":\"%s\"", Key.OTS_ENDPOINT, conf.getEndpoint()));
        }
        if (conf.getAccessId() != null) {
            lines.add(String.format("\t\"%s\":\"%s\"", Key.OTS_ACCESSID, conf.getAccessId()));
        }
        if (conf.getAccesskey() != null) {
            lines.add(String.format("\t\"%s\":\"%s\"", Key.OTS_ACCESSKEY, conf.getAccesskey()));
        }
        if (conf.getInstanceName() != null) {
            lines.add(String.format("\t\"%s\":\"%s\"", Key.OTS_INSTANCE_NAME, conf.getInstanceName()));
        }
        if (conf.getTableName() != null) {
            lines.add(String.format("\t\"%s\":\"%s\"", Key.TABLE_NAME, conf.getTableName()));
        }
        if (conf.getColumns() != null) {
            String cols = columnToString(conf.getColumns());
            if (cols == null) {
                lines.add(String.format("\t\"%s\":[]", Key.COLUMN));
            } else {
                lines.add(String.format("\t\"%s\":[\n%s\t]", Key.COLUMN, cols));
            }
        }
        if (conf.getRangeBegin() != null) {
            String cols = rangeToString(conf.getRangeBegin());
            if (cols == null) {
                lines.add(String.format("\t\"%s\":[]", Key.RANGE_BEGIN));
            } else {
                lines.add(String.format("\t\"%s\":[\n%s\t]", Key.RANGE_BEGIN, cols));
            }    
        }
        if (conf.getRangeEnd() != null) {
            String cols = rangeToString(conf.getRangeEnd());
            if (cols == null) {
                lines.add(String.format("\t\"%s\":[]", Key.RANGE_END));
            } else {
                lines.add(String.format("\t\"%s\":[\n%s\t]", Key.RANGE_END, cols));
            }    
        }
        if (conf.getRangeSplit() != null) {
            String cols = rangeToString(conf.getRangeSplit());
            if (cols == null) {
                lines.add(String.format("\t\"%s\":[]", Key.RANGE_SPLIT));
            } else {
                lines.add(String.format("\t\"%s\":[\n%s\t]", Key.RANGE_SPLIT, cols));
            }    
        }
        if (conf.getRetry() > 0) {
            lines.add(String.format("\t\"%s\":%d", Key.RETRY, conf.getRetry()));
        }
        if (conf.getSleepInMilliSecond() > 0) {
            lines.add(String.format("\t\"%s\":%d", Key.SLEEP_IN_MILLI_SECOND, conf.getSleepInMilliSecond()));
        }
        
        return "{"+ linesToString(lines) +"}";
    }
}
