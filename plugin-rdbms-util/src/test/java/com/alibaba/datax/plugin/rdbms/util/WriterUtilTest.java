package com.alibaba.datax.plugin.rdbms.util;

import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuyi on 15-2-28.
 */
public class WriterUtilTest {

    @Test
    public void getWriteTemplate() {
        List<String> columnHolders = new ArrayList<String>();
        columnHolders.add("cluster");
        columnHolders.add("`group`");
        columnHolders.add("idc");

        List<String> valueHolders = new ArrayList<String>();
        valueHolders.add("?");
        valueHolders.add("?");
        valueHolders.add("?");

        String template = WriterUtil.getWriteTemplate(columnHolders, valueHolders, "replace");
        Assert.assertEquals(template, "replace INTO %s (cluster,`group`,idc) VALUES(?,?,?)");

    }

}
