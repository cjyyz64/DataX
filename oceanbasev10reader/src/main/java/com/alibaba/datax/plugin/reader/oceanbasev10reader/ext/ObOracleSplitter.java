package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.ObVersion;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartInfo;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartType;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartitionSplitUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;

public class ObOracleSplitter extends OceanbaseSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(ObOracleSplitter.class);

    private static final String ORACLE_GET_SUBPART_TEMPLATE = "select subpartition_name " + "from dba_tab_subpartitions " + "where table_name = '%s' and table_owner = '%s'";

    private static final String ORACLE_GET_PART_TEMPLATE = "select partition_name " + "from dba_tab_partitions " + "where table_name = '%s' and table_owner = '%s'";
    private static final String ORACLE_FIRST_SPLIT_TEMPLATE_V1 = "select %s from (select %s, ROWNUM RN from %s %s where %s ROWNUM <= %s order by %s) A where ROWNUM<=1 order by A.RN";

    private static final String ORACLE_APPEND_SPLIT_TEMPLATE_V1 = "select %s from (select %s, ROWNUM RN from %s %s where %s (%s) > (%s) and ROWNUM <= %s order by %s) A where ROWNUM<=1 order by A.RN";

    private static final String ORACLE_FIRST_SPLIT_TEMPLATE_V2 = "select %s, ROWNUM RN from %s %s where %s order by %s OFFSET %s ROWS FETCH NEXT 1 ROWS ONLY";

    private static final String ORACLE_APPEND_SPLIT_TEMPLATE_V2 = "select %s, ROWNUM RN from %s %s where %s (%s) > (%s) order by %s OFFSET %s ROWS FETCH NEXT 1 ROWS ONLY";

    public ObOracleSplitter(Configuration conf) {
        super(conf);
        this.compatibleMode = "ORACLE";
    }

    @Override
    protected boolean isOracleMode() {
        return true;
    }

    @Override
    protected String wrapName(String column) {
        return "\"" + column + "\"";
    }

    @Override
    protected PartInfo queryPartitions(String table, String dbName, Connection conn) {
        PartInfo partInfo;
        // check if the table has subpartitions or not
        String getSubPartSql = String.format(ORACLE_GET_SUBPART_TEMPLATE, table, dbName);
        List<String> partList = ObReaderUtils.getResultsFromSql(conn, getSubPartSql);
        if (partList.size() > 0) {
            partInfo = new PartInfo(PartType.SUBPARTITION);
            partInfo.addPart(partList);
            return partInfo;
        }

        String getPartSql = String.format(ORACLE_GET_PART_TEMPLATE, table, dbName);
        partList = ObReaderUtils.getResultsFromSql(conn, getPartSql);
        if (partList.size() > 0) {
            partInfo = new PartInfo(PartType.PARTITION);
            partInfo.addPart(partList);
            return partInfo;
        }

        // table is not partitioned
        partInfo = new PartInfo(PartType.NONPARTITION);
        return partInfo;
    }

    @Override
    protected String getQuerySql(List<String> splitIndex, String table, String partName, long sliceSize, boolean first) {
        String indexStr = splitIndex.stream().map(this::wrapName).collect(Collectors.joining(","));
        String descIndexStr = splitIndex.stream().map(e -> wrapName(e) + " desc").collect(Collectors.joining(","));
        boolean isPrevious2252 = ObVersion.V2252.compareTo(ObReaderUtils.obVersion) > 0;
        String partInfo = "";
        if (!StringUtils.isEmpty(partName)) {
            partInfo = " partition (" + partName + ")";
        }
        boolean hasWhere = !StringUtils.isEmpty(where);
        String isNotNull = splitIndex.stream().map(e -> e + " is not null").collect(Collectors.joining(" and "));
        String whereClause = hasWhere ? where + " and " : EMPTY;
        whereClause += isNotNull;
        if (first) {
            if (isPrevious2252) {
                whereClause += " and ";
                return String.format(ORACLE_FIRST_SPLIT_TEMPLATE_V1, indexStr, indexStr, table, partInfo, whereClause, sliceSize, descIndexStr);
            }
            return String.format(ORACLE_FIRST_SPLIT_TEMPLATE_V2, indexStr, table, partInfo, whereClause, indexStr, sliceSize - 1);
        }
        whereClause += " and ";
        String placeHolderStr = splitIndex.stream().map(e -> "?").collect(Collectors.joining(","));
        if (isPrevious2252) {
            return String.format(ORACLE_APPEND_SPLIT_TEMPLATE_V1, indexStr, indexStr, table, partInfo, whereClause, indexStr, placeHolderStr, sliceSize, descIndexStr);
        }
        return String.format(ORACLE_APPEND_SPLIT_TEMPLATE_V2, indexStr, table, partInfo, whereClause, indexStr, placeHolderStr, indexStr, sliceSize - 1);
    }
}
