package com.alibaba.datax.core;

import com.alibaba.datax.dataxservice.face.domain.Result;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.junit.Test;

import java.lang.reflect.Type;

/**
 * Date: 2015/5/19 22:45
 *
 * @author Administrator <a href="mailto:liupengjava@gmail.com">Ricoul</a>
 */
public class UtilsTest {
    @Test
    public void testName() throws Exception {
        Integer b= null;
        System.out.println(null == b);
    }

    @Test
    public void testget() throws Exception {
        String resJson = "{\n" +
                "returnCode: 0,\n" +
                "message: \"\",\n" +
                "data: -1,\n" +
                "success: true\n" +
                "}";
        Type type = new TypeReference<Result<Integer>>() {}.getType();
        Result<Integer> result = JSON.parseObject(resJson, type);
        System.out.println(result.getData() == 40);
    }

    @Test
    public void testJson() throws Exception {
        System.out.println(JSON.toJSONString(null));
    }
}
