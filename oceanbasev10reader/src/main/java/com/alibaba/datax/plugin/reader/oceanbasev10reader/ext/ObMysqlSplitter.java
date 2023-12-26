package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.ObVersion;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartInfo;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartType;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.PartitionSplitUtil;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;

public class ObMysqlSplitter extends OceanbaseSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(ObMysqlSplitter.class);

    private static final String MYSQL_GET_PART_TEMPLATE =
            "select p.part_name " + "from oceanbase.__all_part p, oceanbase.%s t, oceanbase.__all_database d " + "where p.table_id = t.table_id " + "and d.database_id = t.database_id "
                    + "and d.database_name = '%s' " + "and t.table_name = '%s'";

    private static final String MYSQL_GET_SUBPART_TEMPLATE =
            "select p.sub_part_name " + "from oceanbase.__all_sub_part p, oceanbase.%s t, oceanbase.__all_database d " + "where p.table_id = t.table_id " + "and d.database_id = t.database_id "
                    + "and d.database_name = '%s' " + "and t.table_name = '%s'";

    private static final String MYSQL_FIRST_SPLIT_TEMPLATE = "select %s from %s %s where %s order by %s limit %s,1";

    private static final String MYSQL_APPEND_SPLIT_TEMPLATE = "select %s from %s %s where %s (%s) > (%s) order by %s limit %s,1";

    public ObMysqlSplitter(Configuration conf) {
        super(conf);
        this.compatibleMode = "MYSQL";
    }

    @Override
    protected boolean isOracleMode() {
        return false;
    }

    @Override
    protected PartInfo queryPartitions(String table, String dbName, Connection conn) {
        PartInfo partInfo = new PartInfo(PartType.NONPARTITION);
        List<String> partList;
        String allTable = "__all_table";
        try {
            final ObVersion version = ObReaderUtils.getObVersion(conn);

            if (version.compareTo(ObVersion.V4000) < 0 && version.compareTo(ObVersion.V2276) >= 0) {
                allTable = "__all_table_v2";
            }

            String querySubPart = String.format(MYSQL_GET_SUBPART_TEMPLATE, allTable, dbName, table);

            PartType partType = PartType.SUBPARTITION;

            // try subpartition first
            partList = ObReaderUtils.getResultsFromSql(conn, querySubPart);

            // if table is not sub-partitioned, the try partition
            if (partList.isEmpty()) {
                String queryPart = String.format(MYSQL_GET_PART_TEMPLATE, allTable, dbName, table);
                partList = ObReaderUtils.getResultsFromSql(conn, queryPart);
                partType = PartType.PARTITION;
            }

            if (!partList.isEmpty()) {
                partInfo = new PartInfo(partType);
                partInfo.addPart(partList);
            }
        } catch (Exception ex) {
            LOG.error("error when get partition list: " + ex.getMessage());
        }
        return partInfo;
    }

    @Override
    protected String getQuerySql(List<String> splitIndex, String table, String partName, long splitSize, boolean first) {
        String indexStr = splitIndex.stream().map(this::wrapName).collect(Collectors.joining(","));
        String partInfo = !StringUtils.isEmpty(partName) ? " partition (" + partName + ")" : EMPTY;
        boolean hasWhere = !StringUtils.isEmpty(where);
        String isNotNull = splitIndex.stream().map(e -> e + " is not null").collect(Collectors.joining(" and "));
        String whereClause = hasWhere ? where + " and " : EMPTY;
        whereClause += isNotNull;
        if (first) {
            return String.format(MYSQL_FIRST_SPLIT_TEMPLATE, indexStr, table, partInfo, whereClause, indexStr, splitSize - 1);
        }
        whereClause += " and ";
        String placeHolderStr = splitIndex.stream().map(e -> "?").collect(Collectors.joining(","));
        return String.format(MYSQL_APPEND_SPLIT_TEMPLATE, indexStr, table, partInfo, whereClause, indexStr, placeHolderStr, indexStr, splitSize - 1);
    }

    @Override
    protected String wrapName(String column) {
        return "`" + column + "`";
    }
}
