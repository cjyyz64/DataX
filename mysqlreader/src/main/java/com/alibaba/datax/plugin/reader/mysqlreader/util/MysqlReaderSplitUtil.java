package com.alibaba.datax.plugin.reader.mysqlreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.TableExpandUtil;
import com.alibaba.datax.plugin.reader.mysqlreader.Constant;
import com.alibaba.datax.plugin.reader.mysqlreader.Key;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public final class MysqlReaderSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(MysqlReaderSplitUtil.class);

    public static List<Configuration> doSplit(Configuration originalSliceConfig,
                                              int adviceNumber) {
        Configuration simplifiedConf = originalSliceConfig;

        boolean isTableMode = simplifiedConf.getBool(Constant.TABLE_MODE)
                .booleanValue();
        int eachTableShouldSplittedNumber = -1;
        if (isTableMode) {
            eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                    adviceNumber,
                    simplifiedConf.getInt(Constant.TABLE_NUMBER_MARK));
        }

        String column = simplifiedConf.getString(Key.COLUMN);
        String where = simplifiedConf.getString(Key.WHERE, null);

        List<Object> conns = simplifiedConf.getList(Constant.CONN_MARK,
                Object.class);

        String jdbcUrl = null;
        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = simplifiedConf.clone();

            Configuration connConf = Configuration
                    .from(conns.get(i).toString());
            jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, appendJDBCSuffix(jdbcUrl));

            sliceConfig.remove(Constant.CONN_MARK);
            
            Configuration tempSlice;

            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                Validate.isTrue(null != tables && !tables.isEmpty(),
                        "source table configed error.");

                String splitPk = simplifiedConf.getString(Key.SPLIT_PK, null);

                // TODO 切分有风险，需要检查最后切分总数，是否等于adviceNum
                boolean needSplitTable = eachTableShouldSplittedNumber > 1
                        && StringUtils.isNotBlank(splitPk);

                if (needSplitTable) {
                    // 尝试对每个表，切分为eachTableShouldSplittedNumber 份
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);

                        List<Configuration> splittedSlices = SingleTableSplitUtil
                                .splitSingleTable(tempSlice,
                                        eachTableShouldSplittedNumber);
                        for (Configuration splittedSlice : splittedSlices) {
                            splittedConfigs.add(splittedSlice);
                        }
                    }
                } else {
                    
                    for (String table : tables) {
                    	tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil
                                .buildQuerySql(column, table, where));
                        splittedConfigs.add(tempSlice);
                    }
                }
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL,
                        String.class);

                // TODO 是否check 配置为多条语句？？
                for (String querySql : sqls) {
                	tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.QUERY_SQL, querySql);
                    splittedConfigs.add(tempSlice);

                }
            }

        }

        return splittedConfigs;
    }

    // TODO 是否允许用户自定义该配置？
    private static String appendJDBCSuffix(String jdbc) {
        String suffix = "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&useCursorFetch=true";

        if (jdbc.contains("?")) {
            return jdbc + "&" + suffix;
        } else {
            return jdbc + "?" + suffix;
        }
    }

    private static boolean isListValueSame(List<Boolean> flags) {
        if (flags.size() == 1) {
            return true;
        }

        boolean isSame = true;
        boolean preValue = flags.get(0);

        for (int i = 1, len = flags.size(); i < len; i++) {
            if (preValue != flags.get(i)) {
                isSame = false;
                break;
            }
        }
        return isSame;
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
                                                              int tableNumber) {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }

    private static List<String> expandTableConf(List<String> tables) {
        List<String> parsedTables = new ArrayList<String>();
        for (String table : tables) {
            List<String> splittedTables = TableExpandUtil.splitTables(table);
            parsedTables.addAll(splittedTables);
        }

        return parsedTables;
    }

    private static String chooseJdbcUrl(List<String> jdbcUrls, String username,
                                        String password) {
        Connection conn = null;
        for (String jdbcUrl : jdbcUrls) {
            // TODO 需要修改其逻辑，不要直接报错
            try {
                conn = DBUtil.getConnection("mysql", jdbcUrl, username,
                        password);
            } catch (Exception e) {
                LOG.warn("jdbcUrl:[{}] not available.", jdbcUrl);
            }
            if (null != conn) {
                return jdbcUrl;
            }
        }

        throw new DataXException(DBUtilErrorCode.CONN_DB_ERROR,
                "no available jdbc.");
    }

}
