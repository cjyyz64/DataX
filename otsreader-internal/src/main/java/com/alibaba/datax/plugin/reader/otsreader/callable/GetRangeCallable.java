package com.alibaba.datax.plugin.reader.otsreader.callable;

import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.internal.OTS;
import com.aliyun.openservices.ots.internal.model.GetRangeRequest;
import com.aliyun.openservices.ots.internal.model.GetRangeResult;
import com.aliyun.openservices.ots.internal.model.RangeRowQueryCriteria;

public class GetRangeCallable implements Callable<GetRangeResult> {
    
    private OTS ots;
    private RangeRowQueryCriteria criteria;
    
    public GetRangeCallable(OTS ots, RangeRowQueryCriteria criteria) {
        this.ots = ots;
        this.criteria = criteria;
    }
    
    @Override
    public GetRangeResult call() throws Exception {
        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        return ots.getRange(request);
    }
}