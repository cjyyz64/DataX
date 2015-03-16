package com.aliyun.openservices.ots.internal.model;

import java.util.List;

public class MockGetRangeResult extends GetRangeResult{
    
    private List<Row> rows = null;
    
    public MockGetRangeResult (List<Row> rows) {
        this.rows = rows;
    }
    
    public GetRangeResult toGetRangeResult() {
        GetRangeResult result = new GetRangeResult();
        result.setRows(rows);
        return result;
    }
}
