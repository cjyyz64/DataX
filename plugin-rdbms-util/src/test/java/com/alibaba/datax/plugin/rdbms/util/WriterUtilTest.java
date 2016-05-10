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

        String template = WriterUtil.getWriteTemplate(columnHolders, valueHolders, "replace",DataBaseType.MySql,false);
        Assert.assertEquals(template, "replace INTO %s (cluster,`group`,idc) VALUES(?,?,?)");


        String template2 = WriterUtil.getWriteTemplate(columnHolders, valueHolders, "replace",null,false);
        Assert.assertEquals(template2, "replace INTO %s (cluster,`group`,idc) VALUES(?,?,?)");


        //dataBase为空,也只能替换成replace
        String template3 = WriterUtil.getWriteTemplate(columnHolders, valueHolders, "update",null,false);
        Assert.assertEquals(template3, "replace INTO %s (cluster,`group`,idc) VALUES(?,?,?)");

        String template4 = WriterUtil.getWriteTemplate(columnHolders, valueHolders, "update",DataBaseType.MySql,false);
        Assert.assertEquals(template4, "INSERT INTO %s (cluster,`group`,idc) VALUES(?,?,?) ON DUPLICATE KEY UPDATE cluster=VALUES(cluster),`group`=VALUES(`group`),idc=VALUES(idc)" );


        String template5 = WriterUtil.getWriteTemplate(columnHolders, valueHolders, "update",DataBaseType.Tddl,false);
        Assert.assertEquals(template5, "INSERT INTO %s (cluster,`group`,idc) VALUES(?,?,?) ON DUPLICATE KEY UPDATE cluster=VALUES(cluster),`group`=VALUES(`group`),idc=VALUES(idc)" );


        String template6 = WriterUtil.getWriteTemplate(columnHolders, valueHolders, "update",DataBaseType.Oracle,false);
        Assert.assertEquals(template6, "replace INTO %s (cluster,`group`,idc) VALUES(?,?,?)");

        //错误的使用了force
        String template7 = WriterUtil.getWriteTemplate(columnHolders, valueHolders, "update",DataBaseType.Oracle,true);
        Assert.assertEquals(template7, "INSERT INTO %s (cluster,`group`,idc) VALUES(?,?,?) ON DUPLICATE KEY UPDATE cluster=VALUES(cluster),`group`=VALUES(`group`),idc=VALUES(idc)");
    }

}
