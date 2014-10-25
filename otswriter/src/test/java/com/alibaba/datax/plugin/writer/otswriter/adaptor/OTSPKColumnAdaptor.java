package com.alibaba.datax.plugin.writer.otswriter.adaptor;

import java.lang.reflect.Type;

import com.alibaba.datax.plugin.writer.otswriter.model.OTSConst;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSErrorMessage;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSPKColumn;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class OTSPKColumnAdaptor implements JsonDeserializer<OTSPKColumn>, JsonSerializer<OTSPKColumn>{
    
    private final static String NAME = "name";
    private final static String TYPE = "type";

    @Override
    public JsonElement serialize(OTSPKColumn src, Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject json = new JsonObject();
        json.add(NAME, new JsonPrimitive(src.getName()));
        
        switch (src.getType()) {
        case STRING:
            json.add(TYPE, new JsonPrimitive(OTSConst.TYPE_STRING));
            break;
        case INTEGER:
            json.add(TYPE, new JsonPrimitive(OTSConst.TYPE_INTEGER));
            break;
        default:
            throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_TYPE_ERROR,  src.getType()));
        }
        return json;
    }

    @Override
    public OTSPKColumn deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        JsonObject obj = json.getAsJsonObject();
        String name = obj.getAsJsonPrimitive(NAME).getAsString();
        String type = obj.getAsJsonPrimitive(TYPE).getAsString();
        
        if (type.equalsIgnoreCase(OTSConst.TYPE_STRING)) {
            return new OTSPKColumn(name, PrimaryKeyType.STRING);
        } else if (type.equalsIgnoreCase(OTSConst.TYPE_INTEGER)) {
            return new OTSPKColumn(name, PrimaryKeyType.INTEGER);
        } else {
            throw new IllegalArgumentException(String.format(OTSErrorMessage.PK_TYPE_ERROR,  type));
        }
    }
}
