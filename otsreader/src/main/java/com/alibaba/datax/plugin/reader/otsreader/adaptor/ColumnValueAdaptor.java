package com.alibaba.datax.plugin.reader.otsreader.adaptor;

import java.lang.reflect.Type;

import org.apache.commons.codec.binary.Base64;

import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * {"type":"STRING", "value":"hello"}
 * {"type":"INTEGER", "value":"1111"}
 * {"type":"BOOLEAN", "value":"11.0"}
 * {"type":"DOUBLE", "value":"true"}
 * {"type":"BINARY", "value":"Base64(bin)"}
 */
public class ColumnValueAdaptor implements JsonDeserializer<ColumnValue>, JsonSerializer<ColumnValue>{
    private final static String TYPE = "type";
    private final static String VALUE = "value";

    @Override
    public JsonElement serialize(ColumnValue obj, Type t,
            JsonSerializationContext c) {
        JsonObject json = new JsonObject();

        switch (obj.getType()) {
        case STRING : 
            json.add(TYPE, new JsonPrimitive(ColumnType.STRING.toString())); 
            json.add(VALUE, new JsonPrimitive(obj.asString()));
            break;
        case INTEGER : 
            json.add(TYPE, new JsonPrimitive(ColumnType.INTEGER.toString())); 
            json.add(VALUE, new JsonPrimitive(obj.asLong()));
            break;
        case BOOLEAN : 
            json.add(TYPE, new JsonPrimitive(ColumnType.BOOLEAN.toString())); 
            json.add(VALUE, new JsonPrimitive(obj.asBoolean()));
            break;
        case DOUBLE : 
            json.add(TYPE, new JsonPrimitive(ColumnType.DOUBLE.toString())); 
            json.add(VALUE, new JsonPrimitive(obj.asDouble()));
            break;
        case BINARY : 
            json.add(TYPE, new JsonPrimitive(ColumnType.BINARY.toString())); 
            json.add(VALUE, new JsonPrimitive(Base64.encodeBase64String(obj.asBinary())));
            break;
        }
        return json;
    }

    @Override
    public ColumnValue deserialize(JsonElement ele, Type t,
            JsonDeserializationContext c) throws JsonParseException {
        JsonObject obj = ele.getAsJsonObject();
        String strType = obj.getAsJsonPrimitive(TYPE).getAsString();
        String strValue =  obj.getAsJsonPrimitive(VALUE).getAsString();
        ColumnType type = ColumnType.valueOf(strType);
        
        ColumnValue value = null;
        switch(type) {
        case STRING : 
            value = ColumnValue.fromString(strValue);
            break;
        case INTEGER : 
            value = ColumnValue.fromLong(Long.parseLong(strValue));
            break;
        case BOOLEAN :
            value = ColumnValue.fromBoolean(Boolean.parseBoolean(strValue));
            break;
        case DOUBLE : 
            value = ColumnValue.fromDouble(Double.parseDouble(strValue));
            break;
        case BINARY : 
            value = ColumnValue.fromBinary(Base64.decodeBase64(strValue));
            break;
        }
        return value;
    }
}
