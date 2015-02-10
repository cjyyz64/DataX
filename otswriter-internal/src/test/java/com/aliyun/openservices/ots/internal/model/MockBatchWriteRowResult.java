package com.aliyun.openservices.ots.internal.model;

public class MockBatchWriteRowResult extends BatchWriteRowResult{

    public MockBatchWriteRowResult(OTSResult meta) {
        super(meta);
    }
 
    public void addPutRowResult(RowResult status) {
        super.addPutRowResult(status);
    }

    public void addUpdateRowResult(RowResult status) {
        super.addUpdateRowResult(status);
    }

    public void addDeleteRowResult(RowResult status) {
        super.addDeleteRowResult(status);
    }
}
