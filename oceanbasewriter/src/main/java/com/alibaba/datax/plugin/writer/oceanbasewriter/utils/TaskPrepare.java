package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.oceanbasewriter.Key;
import com.alibaba.datax.plugin.writer.oceanbasewriter.Rowkey;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TaskPrepare {

    private static final Logger log = LoggerFactory.getLogger(TaskPrepare.class);

    public static void master(Configuration configuration) throws Exception{
        List<String> prepareSQL = configuration.getList(Key.PRE_SQL, Collections.<String>emptyList(), String.class);
        if(prepareSQL.isEmpty()) return;
        log.info("master prepare start");
        List<Configuration> tasks = TaskSplitter.split(configuration);
        if(tasks.size() == 1) {
            configuration.remove(Key.PRE_SQL);
            Configuration conf = tasks.get(0);
            OBDataSource.init(conf);
            Helper.delete(conf, prepareSQL);
            OBDataSource.destroy(conf);
        }
        log.info("master prepare end");
    }

    public static void slave(Configuration configuration) throws Exception{
        List<String> prepareSQL = configuration.getList(Key.PRE_SQL, Collections.<String>emptyList(),String.class);
        if(prepareSQL.isEmpty()) return;
        OBDataSource.init(configuration);
        log.info("slave[{}] prepare start",Thread.currentThread().getName());
        Helper.delete(configuration,prepareSQL);
        log.info("slave[{}] prepare end",Thread.currentThread().getName());
        OBDataSource.destroy(configuration);
    }

    private static class Helper{

        private static final String tip_1 = "only support delete in preSql,your sql [%s]";
        private static final String tip_2 = "miss keyword from, your sql [%s]";
        private static final int limit = 500;

        public static void delete(Configuration configuration, List<String> deletes) throws Exception{
            String url = configuration.getString(Key.CONFIG_URL);
            if (Helper.lowVersion(url)){
                throw new RuntimeException("only support OB 0.5 version or newer");
            }
            for (String sql : deletes){
                String delete = sql.trim().toLowerCase();
                Preconditions.checkArgument(delete.startsWith("delete "), String.format(tip_1,sql));
                Preconditions.checkArgument(delete.contains(" from "), String.format(tip_2,sql));
                String table = table(configuration, delete);
                String where = where(delete);
                String hint = String.format("/*+index(%s primary)*/",table);
                Rowkey rowkey = rowkey(url,table);
                String fields = field(rowkey);
                String query = String.format("select %s %s from %s %s limit %s", hint, fields, table, where, limit);
                log.info("{} start execute delete table {}",Thread.currentThread().getName(),table);
                DeleteHandler handler = new DeleteHandler(table, rowkey, url);
                String condition = OBDataSource.executeQuery(url, query, handler);
                while (!"".equals(condition)) {
                    if(where.equals("")){
                        query = String.format("select %s %s from %s where %s limit %s", hint, fields, table, condition, limit);
                    }else {
                        query = String.format("select %s %s from %s %s and %s limit %s", hint, fields, table, where, condition, limit);
                    }
                    condition = OBDataSource.executeQuery(url, query, handler);
                }
                log.info("delete total {} records for table {} ", handler.counter.get(), table);
            }
        }

        private static String field(Rowkey rowkey){
            StringBuilder builder = new StringBuilder();
           for (Rowkey.Entry index : rowkey){
              builder.append(index.name).append(",");
           }
            return builder.deleteCharAt(builder.length() - 1 ).toString();
        }

        private static String table(Configuration configuration,String delete){
            if (delete.contains(" @table"))
                return configuration.getString(Key.TABLE);
            boolean haveWhere = delete.contains(" where ");
            if (haveWhere){
                return delete.substring(delete.lastIndexOf(" from ") + 6, delete.indexOf(" where ")).trim();
            }else {
                return delete.substring(delete.lastIndexOf(" from ") + 6);
            }
        }

        private static String where(String delete){
            boolean haveWhere = delete.contains(" where ");
            if (haveWhere){
                String condition = delete.substring(delete.lastIndexOf(" where ") + 7).trim();
                return String.format("where (%s)",condition);
            }else {
                return  "";
            }
        }

        public static boolean lowVersion(String url) throws Exception {
            ResultSetHandler<Boolean> handler = new ResultSetHandler<Boolean>() {
                @Override
                public Boolean callback(ResultSet result) throws SQLException {
                    result.next();
                    String version = result.getString("value");
                    return version.contains("0.4");
                }
            };
            return OBDataSource.executeQuery(url,"show variables like 'version_comment'", handler);
        }

        public static Rowkey rowkey(String url, String table) throws Exception {
            ResultSetHandler<Rowkey> handler = new ResultSetHandler<Rowkey>() {

                @Override
                public Rowkey callback(ResultSet result) throws Exception {
                    Rowkey.Builder builder = Rowkey.builder();
                    while (result.next()) {
                        String name = result.getString("field");
                        String type = result.getString("type");
                        int key = result.getInt("key");
                        if (key != 0)
                            builder.addEntry(name, type, key);
                    }
                    return builder.build();
                }
            };
            return OBDataSource.executeQuery(url, String.format("desc %s", table), handler);
        }

        private static class DeleteHandler implements ResultSetHandler<String> {

            private final Rowkey rowkey;
            private final String table;
            private final String url;
            public final AtomicLong counter = new AtomicLong();
            private DeleteHandler(String table, Rowkey rowkey,String url) throws Exception {
                this.table = table;
                this.rowkey = rowkey;
                this.url = url;
            }

            @Override
            public String callback(ResultSet result) throws Exception {
                this.delete(url, result);
                return this.getLastRowkey(result);
            }

            private void delete(String url,ResultSet result) throws Exception{
                while (result.next()){
                    final String delete = String.format("delete from %s where %s", table, relation("=", result));
                    OBDataSource.execute(url,new ConnectionHandler() {
                        @Override
                        public Statement callback(Connection connection) throws Exception {
                            Statement statement = connection.createStatement();
                            log.debug(delete);
                            statement.execute(delete);
                            return statement;
                        }
                    });
                    counter.incrementAndGet();
                }
            }

            private String getLastRowkey(ResultSet result) throws Exception {
                if (!result.isAfterLast())//根据jdbc规范判断空集
                    return "";
                result.last();
                return relation(">",result);
            }

            private String relation(String op,ResultSet result) throws Exception{
                String left = "(";
                for (Rowkey.Entry entry : rowkey) {
                    if (entry.position == 1) {
                        left += entry.name;
                    } else {
                        left += ("," + entry.name);
                    }
                }
                left += ")";
                String right = "(";
                for (Rowkey.Entry entry : rowkey) {
                    if (entry.position == 1) {
                        right += entry.type.convert(result, entry.name);
                    } else {
                        right += ("," + entry.type.convert(result, entry.name));
                    }
                }
                right += ")";
                return String.format("%s %s %s",left,op,right);
            }
        }
    }
}
