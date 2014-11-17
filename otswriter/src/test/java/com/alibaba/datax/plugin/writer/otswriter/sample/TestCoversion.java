package com.alibaba.datax.plugin.writer.otswriter.sample;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.plugin.writer.otswriter.common.Person;

public class TestCoversion {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Person person = new Person();
        person.setName("redchen");
        person.setAge(100);
        person.setHeight(180);
        
        //Column col = new BytesColumn(person.toByte(person));
        Column col = new StringColumn("");
        //Column col = new BoolColumn(true);
        //Column col = new DoubleColumn(1.0);
        
        try {
            System.out.println("asString:" + col.asString());
        } catch (Exception e) {
            
        }
        try {
            System.out.println("asLong:" + col.asLong());
        } catch (Exception e) {
            
        }
        try {
            System.out.println("asDouble:" + col.asDouble());
        } catch (Exception e) {
            
        }
        try {
            System.out.println("asBoolean:" + col.asBoolean());
        } catch (Exception e) {
            
        }
        try {
            System.out.println("asBytes:" + col.asBytes());
        } catch (Exception e) {
            
        }
    }

}
