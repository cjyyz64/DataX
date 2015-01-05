package com.alibaba.datax.core.util;

import com.alibaba.fastjson.JSON;

import java.lang.reflect.Type;

public class SerializationUtil {

//    private static final Logger LOGGER = LoggerFactory.getLogger(SerializationUtil.class);
//
//    private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
//
//    public static final Gson longDateGson = new GsonBuilder()
//            .registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
//                public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
//                        throws JsonParseException {
//                    return new Date(json.getAsJsonPrimitive().getAsLong());
//                }
//            }).setDateFormat(DateFormat.LONG).create();
//
//    public static <T> T longDateGson2Object(String jsonString, Type type) {
//        try {
//            return longDateGson.fromJson(jsonString, type);
//        } catch (JsonSyntaxException e) {
//            LOGGER.warn("Gson string to object error, string : {} to object type {}", jsonString, type);
//            throw  e;
//        }
//    }
//
//    public static String gson2String(Object object) {
//        return gson.toJson(object);
//    }
//
//    /**
//     * 获取 new TypeToken<Collection<Integer>>(){}.getType();
//     **/
//    public static <T> T gson2Object(String jsonString, Type type) {
//        try {
//            return gson.fromJson(jsonString, type);
//        } catch (JsonSyntaxException e) {
//            LOGGER.warn("Gson string to object error, string : {} to object type {}", jsonString, type);
//            throw  e;
//        }
//    }

    public static <T> T json2Object(String jsonString, Type type) {
        return JSON.parseObject(jsonString,type);
    }

    public static String object2Json(Object object) {
        return JSON.toJSONString(object);
    }
}