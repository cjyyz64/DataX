package com.alibaba.datax.plugin.reader.drdsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

public class DrdsReaderSplitUtil {

    private static final Logger LOG = LoggerFactory
            .getLogger(DrdsReaderSplitUtil.class);

    public static List<Configuration> doSplit(Configuration originalSliceConfig,
                                              int adviceNumber) {
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();
        int tableNumber = originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK);

        if (isTableMode && tableNumber == 1) {
            //需要先把内层的 table,connection 先放到外层
            String table = originalSliceConfig.getString(String.format("%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE)).trim();
            originalSliceConfig.set(Key.TABLE, table);

            originalSliceConfig.remove(Constant.CONN_MARK);
            return doDrdsReaderSplit(originalSliceConfig);
        } else {
            throw DataXException.asDataXException(DrdsReaderErrorCode.CONFIG_ERROR, "Drdsreader 只能配置为读取一张表,后台会通过 Proxy 自动获取实际对应物理表的数据.");
        }
    }

    private static List<Configuration> doDrdsReaderSplit(Configuration originalSliceConfig) {
        List<Configuration> splittedConfigurations = new ArrayList<Configuration>();

        Map<String, List<String>> topology = getTopology(originalSliceConfig);
        if (null == topology || topology.isEmpty()) {
            splittedConfigurations.add(originalSliceConfig);
            return splittedConfigurations;
        } else {
            String table = originalSliceConfig.getString(Key.TABLE).trim();
            String column = originalSliceConfig.getString(Key.COLUMN).trim();
            String where = originalSliceConfig.getString(Key.WHERE, null);
            // 不能带英语分号结尾
            String sql = SingleTableSplitUtil
                    .buildQuerySql(column, table, where);
            // 根据拓扑拆分任务
            for (Map.Entry<String, List<String>> entry : topology.entrySet()) {
                String group = entry.getKey();
                StringBuilder sqlbuilder = new StringBuilder();
                sqlbuilder.append("/*+TDDL({'extra':{'MERGE_UNION':'false'},'type':'direct',");
                sqlbuilder.append("'vtab':'").append(table).append("',");
                sqlbuilder.append("'dbid':'").append(group).append("',");
                sqlbuilder.append("'realtabs':[");
                Iterator<String> it = entry.getValue().iterator();
                while (it.hasNext()) {
                    String realTable = it.next();
                    sqlbuilder.append('\'').append(realTable).append('\'');
                    if (it.hasNext()) {
                        sqlbuilder.append(',');
                    }
                }
                sqlbuilder.append("]})*/");
                sqlbuilder.append(sql);
                Configuration param = originalSliceConfig.clone();
                param.set(Key.QUERY_SQL, sqlbuilder.toString());
                splittedConfigurations.add(param);
            }

            return splittedConfigurations;
        }
    }


    private static Map<String, List<String>> getTopology(Configuration configuration) {
        Map<String, List<String>> topology = new HashMap<String, List<String>>();

        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String logicTable = configuration.getString(Key.TABLE).trim();

        Connection conn = null;
        ResultSet rs = null;
        try {
            //TODO  DataBaseType.Drds
            conn = DBUtil.getConnection(DataBaseType.MySql, jdbcURL, username, password);
            rs = DBUtil.query(conn, "SHOW TOPOLOGY " + logicTable);
            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                String tableName = rs.getString("TABLE_NAME");
                List<String> tables = topology.get(groupName);
                if (tables == null) {
                    tables = new ArrayList<String>();
                    topology.put(groupName, tables);
                }
                tables.add(tableName);
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.error("getTopology failed:", e);
            }

            LOG.warn("切分 drds 表失败, 采用不切分模式运行.");
        } finally {
            DBUtil.closeDBResources(rs, null, conn);
        }
        return topology;
    }

}

