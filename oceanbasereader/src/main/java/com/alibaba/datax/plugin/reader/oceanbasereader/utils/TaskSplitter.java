package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.oceanbasereader.Key;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TaskSplitter {

    public static List<Configuration> split(Configuration configuration){
        List<Configuration> slices = Lists.newArrayList();
        List<JSONObject> connections = configuration.getList(Key.CONNECTION,JSONObject.class);
        for (JSONObject connection : connections){
            slices.addAll(slice(connection, configuration));
            Collections.shuffle(slices);
        }
        return slices.size() == 1? handleSingleTableCase(slices.get(0)) : slices;
    }

    private static List<Configuration> slice(JSONObject json,Configuration orgin){
        List<Configuration> slices = Lists.newArrayList();
        Configuration configuration = Configuration.from(json);
        List<String> sqls = configuration.getList(Key.SQL, Collections.<String>emptyList(),String.class);
        for (String sql : sqls){
            Configuration slice = orgin.clone();
            slice.remove(Key.CONNECTION);
            slice.set(Key.SQL,sql);
            slice.set(Key.CONFIG_URL,configuration.get(Key.CONFIG_URL));
            slices.add(slice);
        }
        if(!slices.isEmpty()) return slices;
        List<String> tables = configuration.getList(Key.TABLE, Collections.<String>emptyList(),String.class);
        for (String table : tables){
            Configuration slice = orgin.clone();
            slice.remove(Key.CONNECTION);
            slice.set(Key.TABLE,table);
            slice.set(Key.CONFIG_URL,configuration.get(Key.CONFIG_URL));
            slices.add(slice);
        }
        return slices;
    }

    private static List<Configuration> handleSingleTableCase(Configuration cfg){
        try {
            String url = cfg.getString(Key.CONFIG_URL);
            if (JDBCDataSource.lowVersion(url)){
                return ImmutableList.of(cfg);
            }else {
                return TabletSplitter.slices(cfg);
            }
        }catch (Exception e){
           throw new RuntimeException("split single table task error",e);
        }
    }

    private static class TabletSplitter {
        private static final String meta_template = "select * from __%s__meta__";
        public static List<Configuration> slices(Configuration cfg) throws Exception{
            String table = cfg.getString(Key.TABLE);
            String url = cfg.getString(Key.CONFIG_URL);
            String tableId = JDBCDataSource.tableId(url, table);
            final RowkeyMeta meta = JDBCDataSource.rowkey(url, table, tableId);
            Set<Tablet> tablets = JDBCDataSource.execute(url,String.format(meta_template,tableId),new ResultSetHandler<Set<Tablet>>() {
                @Override
                public Set<Tablet> callback(ResultSet result) throws Exception {
                    Set<Tablet> tablets = Sets.newHashSet();
                    while (result.next()) {
                        List<Object> startRowkey = Lists.newArrayList();
                        List<Object> endRowkey = Lists.newArrayList();
                        for (RowkeyMeta.Entry entry : meta) {
                            startRowkey.add(fetchValue(entry,result,String.format("startkey_%s", entry.id)));
                            endRowkey.add(fetchValue(entry,result,String.format("endkey_%s", entry.id)));
                        }
                        tablets.add(new Tablet(meta, Collections.unmodifiableList(startRowkey), Collections.unmodifiableList(endRowkey)));
                    }
                    return tablets;
                }
            });
            List<Configuration> slices = Lists.newArrayListWithCapacity(tablets.size());
            for (Tablet tablet : tablets){
                Configuration slice = cfg.clone();
                slice.set("startkey",tablet.startkey);
                slice.set("endkey",tablet.endkey);
                slices.add(slice);
            }
            return slices;
        }

        private static Object fetchValue(RowkeyMeta.Entry entry,ResultSet result,String name) throws Exception {
            String value = result.getString(name);
            if(value == null) return null;
            if(RowkeyMeta.BoundValue.isMin(value)) return RowkeyMeta.BoundValue.OB_MIN;
            if(RowkeyMeta.BoundValue.isMax(value)) return RowkeyMeta.BoundValue.OB_MAX;
            return entry.value(result,name);
        }
    }

}
