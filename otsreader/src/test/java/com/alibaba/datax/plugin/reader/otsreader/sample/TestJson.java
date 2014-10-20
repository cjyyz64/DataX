package com.alibaba.datax.plugin.reader.otsreader.sample;

import org.apache.commons.lang3.StringEscapeUtils;

public class TestJson {

    public static void main(String[] args) {
        String ss = "\\\\:";
        System.out.println(ss);
        System.out.println(StringEscapeUtils.escapeJson(ss));
    }

}
