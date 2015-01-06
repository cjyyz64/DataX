package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public final class JDBCDataSource {

	private static final Logger log = LoggerFactory.getLogger(OBDataSource.class);
    private static final String JDBC_URL = "jdbc:mysql://%s/datax?emulateUnsupportedPstmts=false&characterEncoding=UTF-8&useServerPrepStmts=true&prepStmtCacheSqlLimit=1000&enableQueryTimeouts=false&useLocalSessionState=false&useLocalTransactionState=false";

	public static <T> T execute(String url, String sql, ResultSetHandler<T> handler) throws Exception {
		int retry = 0;
		while(retry++ <= 100){
			Connection connection = null;
			Statement statement = null;
			ResultSet result = null;
			try {
                String lms = fetchMasterLMS(url);
                String db = String.format(JDBC_URL,lms);
                connection = DriverManager.getConnection(db, "monitor", "ocenabasev5_monitor");
				statement = connection.createStatement();
				log.info("start execute: {}", sql);
				result = statement.executeQuery(sql);
				return handler.callback(result);
			} catch(Exception e){
				if(retry == 100) log.error(String.format("execute sql [%s] exception and exit", sql), e);
			}finally {
                if(result != null) result.close();
                if(statement != null) statement.close();
                if(connection != null) connection.close();
			}
		}
		throw new Exception(String.format("retry sql [%s] fail exit", sql));
	}

    public static RowkeyMeta rowkey(final String url, final String table,final String tableId) throws Exception {

        class Helper{

            class Handler implements ResultSetHandler<String>{

                @Override
                public String callback(ResultSet result) throws Exception {
                    if (result.next()){
                        return result.getString("column_id");
                    }
                    throw new IllegalArgumentException(String.format("can not find table [%s] column_id",table));
                }
            }

            private String template = "select column_id from __all_column where table_id = %s and column_name = '%s'";

            public String fetchColumnId(String url, String column, String table_id) throws Exception{
                String sql = String.format(template,table_id,column);
                return  execute(url ,sql,new Handler());
            }

        }

        ResultSetHandler<RowkeyMeta> handler = new ResultSetHandler<RowkeyMeta>() {
            private Helper helper = new Helper();
            @Override
            public RowkeyMeta callback(ResultSet result) throws Exception {
                RowkeyMeta.Builder builder = RowkeyMeta.builder();
                while (result.next()) {
                    String name = result.getString("field");
                    String type = result.getString("type");
                    int key = result.getInt("key");
                    if (key != 0){
                        builder.addEntry(name, type,helper.fetchColumnId(url, name,tableId), key);
                    }
                }
                return builder.build();
            }
        };
        return execute(url,String.format("desc %s", table), handler);
    }

    private static final String find_table_id_template = "select table_id from __all_table where table_name = '%s'";

    public static String tableId(String url,final String table) throws Exception{
        return execute(url, String.format(find_table_id_template,table), new ResultSetHandler<String>() {
            @Override
            public String callback(ResultSet result) throws Exception {
                if(result.next()){
                    return result.getString("table_id");
                }
                throw new IllegalArgumentException(String.format("table not exist: [%s]",table));
            }
        });
    }

    private static final Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final String FETCH_MASTER_LMS = "select cluster_vip,cluster_port from __all_cluster where cluster_role = 1";
    private static String fetchMasterLMS(String url) throws Exception{
        Properties properties = new Properties();
        properties.load(new URL(url).openStream());
        for(String address : splitter.split((String)properties.get("clusterAddress"))){
            Connection connection = null;
            Statement statement = null;
            ResultSet result = null;
            try{
                String db = String.format("jdbc:mysql://%s",address);
                connection = DriverManager.getConnection(db, "monitor", "ocenabasev5_monitor");
                statement = connection.createStatement();
                result = statement.executeQuery(FETCH_MASTER_LMS);
                if (result.next()){
                   return result.getString("cluster_vip") + ":" + result.getString("cluster_port");
                }
            }catch (Exception e){
                log.error("fetch master lms error", e);
            }finally {
                if(result != null) result.close();
                if(statement != null) statement.close();
                if(connection != null) connection.close();
            }
        }
        throw new IllegalArgumentException(String.format("can't find master lms for [%s]",url));
    }

    public static boolean lowVersion(String url) throws Exception{
       return execute(url,OBVersionHandler.version,new OBVersionHandler());
    }
}
