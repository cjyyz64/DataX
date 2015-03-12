package com.alibaba.datax.plugin.reader.otsreader.perf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.reader.otsreader.model.OTSRange;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

public class RangeParse {
    
    private List<PrimaryKeyValue> pks = new ArrayList<PrimaryKeyValue>();
    
    public RangeParse(String path) {
        File file = new File(path);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                pks.add(PrimaryKeyValue.fromString(tempString.trim()));
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ee) {
                    ee.printStackTrace();
                }
            }
        }
    }
    
    public List<OTSRange> getRange() {
        List<OTSRange> results = new ArrayList<OTSRange>();
        for (int i = 0; i < pks.size() - 1; i++) {
            RowPrimaryKey begin = new RowPrimaryKey();
            begin.addPrimaryKeyColumn("userid", pks.get(i));
            begin.addPrimaryKeyColumn("groupid", PrimaryKeyValue.INF_MIN);
            RowPrimaryKey end = new RowPrimaryKey();
            end.addPrimaryKeyColumn("userid", pks.get(i + 1));
            end.addPrimaryKeyColumn("groupid", PrimaryKeyValue.INF_MIN);
            
            OTSRange range = new OTSRange();
            range.setBegin(begin);
            range.setEnd(end);
            results.add(range);
        }
        return results;
    }
}
