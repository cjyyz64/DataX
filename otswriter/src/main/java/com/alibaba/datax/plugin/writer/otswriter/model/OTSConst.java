package com.alibaba.datax.plugin.writer.otswriter.model;

public class OTSConst {
    // Reader support type
    public final static String TYPE_STRING  = "STRING";
    public final static String TYPE_INTEGER = "INT";
    public final static String TYPE_DOUBLE  = "DOUBLE";
    public final static String TYPE_BOOLEAN = "BOOL";
    public final static String TYPE_BINARY  = "BINARY";
    
    // Column
    public final static String NAME = "name";
    public final static String TYPE = "type";
    
    public final static String OTS_CONF = "OTS_CONF";
    
    public final static String OTS_OP_TYPE_PUT = "PutRow";
    public final static String OTS_OP_TYPE_UPDATE = "UpdateRow";
}
