package com.alibaba.datax.core.util;

import com.alibaba.datax.common.util.Configuration;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class JobShuffleUtilTest {
    public static void main(String[] args) throws Exception {
        LinkedHashMap<String, List<Integer>> map = new LinkedHashMap<String, List<Integer>>();
        List<Integer> aList = new ArrayList<Integer>();
        aList.add(0);
        aList.add(1);
        aList.add(2);
        map.put("a", aList);
        List<Integer> bList = new ArrayList<Integer>();
        bList.add(3);
        bList.add(4);
        map.put("b", bList);


        List<Integer> cList = new ArrayList<Integer>();
        cList.add(5);
        cList.add(6);
        cList.add(7);
        map.put("c", cList);

        List<Configuration> contentConfig = new LinkedList<Configuration>();
        contentConfig.add(Configuration.from("{\"taskId\":0}"));
        contentConfig.add(Configuration.from("{\"taskId\":1}"));
        contentConfig.add(Configuration.from("{\"taskId\":2}"));
        contentConfig.add(Configuration.from("{\"taskId\":3}"));
        contentConfig.add(Configuration.from("{\"taskId\":4}"));
        contentConfig.add(Configuration.from("{\"taskId\":5}"));
        contentConfig.add(Configuration.from("{\"taskId\":6}"));
        contentConfig.add(Configuration.from("{\"taskId\":7}"));

        Class c = Class.forName(JobShuffleUtil.class.getCanonicalName());
        Method[] abc = c.getDeclaredMethods();
        for (Method m : abc) {
            Type[] types = m.getGenericParameterTypes();
            System.out.println(m.getName());
            for (Type type : types) {
                System.out.println("\t" + type.toString());
            }
        }
        Method m = JobShuffleUtil.class.getDeclaredMethod("doShuffle", new Class[]{map.getClass(), contentConfig.getClass()});
        List<Configuration> result = (List<Configuration>) m.invoke(c, new Object[]{map, contentConfig});

//        List<Configuration> result = doShuffle(map, contentConfig);
        for (Configuration configuration : result) {
            System.out.println(configuration.toString());
        }
    }
}
