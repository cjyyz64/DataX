package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.core.scaffold.RecordProducer;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * no comments.
 * Created by liqiang on 16/3/17.
 */
public class FilterTransformerTest {

    @Test
    public void testLikeNotLike() throws Exception {
        Record record = RecordProducer.produceRecord();
        FilterTransformer filterTransformer = new FilterTransformer();

        /**
         * 字符串
         */

        //条件匹配，过滤
        Record result1 = filterTransformer.evaluate(record, 1, "like", ".*azhe.*");
        Assert.assertEquals(result1, null);


        //条件不匹配，不过滤
        Record result2 = filterTransformer.evaluate(record, 1, "like", ".*azhe.*a");
        Assert.assertEquals(result2, record);


        //条件匹配，过滤
        Record result3 = filterTransformer.evaluate(record, 1, "not like", "1.*azhe.*1");
        Assert.assertEquals(result3, null);


        //条件不匹配，不过滤
        Record result4 = filterTransformer.evaluate(record, 1, "not like", ".*azhe.*");
        Assert.assertEquals(result4, record);


        /**
         * long
         */

        //条件匹配，过滤
        Record result21 = filterTransformer.evaluate(record, 0, "like", "1");
        Assert.assertEquals(result21, null);

        //条件不匹配，不过滤
        Record result22 = filterTransformer.evaluate(record, 0, "like", "2");
        Assert.assertEquals(result22, record);


        //条件匹配，过滤
        Record result23 = filterTransformer.evaluate(record, 0, "not like", "2");
        Assert.assertEquals(result23, null);

        //条件不匹配，不过滤
        Record result24 = filterTransformer.evaluate(record, 0, "not like", ".*");
        Assert.assertEquals(result24, record);


        /**
         * boolean
         */

        //条件匹配，过滤
        Record result31 = filterTransformer.evaluate(record, 2, "like", "true");
        Assert.assertEquals(result31, null);

        //条件不匹配，不过滤
        Record result32 = filterTransformer.evaluate(record, 2, "like", "True");
        Assert.assertEquals(result32, record);


        //条件匹配，过滤
        Record result33 = filterTransformer.evaluate(record, 2, "not like", "false");
        Assert.assertEquals(result33, null);

        //条件不匹配，不过滤
        Record result34 = filterTransformer.evaluate(record, 2, "not like", ".*");
        Assert.assertEquals(result34, record);


        /**
         * date
         */

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String value = simpleDateFormat.format(new Date());

        String errorValue = simpleDateFormat.format(new Date(System.currentTimeMillis() + 86400000));

        //条件匹配，过滤
        Record result41 = filterTransformer.evaluate(record, 3, "like", value + ".*");
        Assert.assertEquals(result41, null);

        //条件不匹配，不过滤
        Record result42 = filterTransformer.evaluate(record, 3, "like", errorValue + ".*");
        Assert.assertEquals(result42, record);


        //条件匹配，过滤
        Record result43 = filterTransformer.evaluate(record, 3, "not like", errorValue + ".*");
        Assert.assertEquals(result43, null);

        //条件不匹配，不过滤
        Record result44 = filterTransformer.evaluate(record, 3, "not like", value + ".*");
        Assert.assertEquals(result44, record);


        /**
         * bytes
         */

        //条件匹配，过滤
        Record result51 = filterTransformer.evaluate(record, 4, "like", ".*azhe.*");
        Assert.assertEquals(result51, null);


        //条件不匹配，不过滤
        Record result52 = filterTransformer.evaluate(record, 4, "like", ".*azhe.*a");
        Assert.assertEquals(result52, record);


        //条件匹配，过滤
        Record result53 = filterTransformer.evaluate(record, 4, "not like", "1.*azhe.*1");
        Assert.assertEquals(result53, null);


        //条件不匹配，不过滤
        Record result54 = filterTransformer.evaluate(record, 4, "not like", ".*azhe.*");
        Assert.assertEquals(result54, record);
    }

    @Test
    public void testEquals() throws Exception {
        Record record = RecordProducer.produceRecord();
        FilterTransformer filterTransformer = new FilterTransformer();

        /**
         * 字符串
         */

        //条件匹配，过滤
        Record result1 = filterTransformer.evaluate(record, 1, "=", "bazhen");
        Assert.assertEquals(result1, null);


        //条件不匹配，不过滤
        Record result2 = filterTransformer.evaluate(record, 1, "=", "anyone");
        Assert.assertEquals(result2, record);


        //条件匹配，过滤
        Record result3 = filterTransformer.evaluate(record, 1, "!=", "anyone");
        Assert.assertEquals(result3, null);


        //条件不匹配，不过滤
        Record result4 = filterTransformer.evaluate(record, 1, "!=", "bazhen");
        Assert.assertEquals(result4, record);


        /**
         * long
         */

        //条件匹配，过滤
        Record result21 = filterTransformer.evaluate(record, 0, "=", "1");
        Assert.assertEquals(result21, null);

        //条件不匹配，不过滤
        Record result22 = filterTransformer.evaluate(record, 0, "=", "2");
        Assert.assertEquals(result22, record);


        //条件匹配，过滤
        Record result23 = filterTransformer.evaluate(record, 0, "!=", "2");
        Assert.assertEquals(result23, null);

        //条件不匹配，不过滤
        Record result24 = filterTransformer.evaluate(record, 0, "!=", "1");
        Assert.assertEquals(result24, record);


        /**
         * boolean
         */

        //条件匹配，过滤
        Record result31 = filterTransformer.evaluate(record, 2, "==", "true");
        Assert.assertEquals(result31, null);

        //条件不匹配，不过滤
        Record result32 = filterTransformer.evaluate(record, 2, "==", "false");
        Assert.assertEquals(result32, record);


        //条件匹配，过滤
        Record result33 = filterTransformer.evaluate(record, 2, "!=", "false");
        Assert.assertEquals(result33, null);

        //条件不匹配，不过滤
        Record result34 = filterTransformer.evaluate(record, 2, "!=", "true");
        Assert.assertEquals(result34, record);


        /**
         * date
         */


        long value = record.getColumn(3).asLong();
        String errorValue = (value - 1) + "";

        //条件匹配，过滤
        Record result41 = filterTransformer.evaluate(record, 3, "==", value + "");
        Assert.assertEquals(result41, null);

        //条件不匹配，不过滤
        Record result42 = filterTransformer.evaluate(record, 3, "==", errorValue+"");
        Assert.assertEquals(result42, record);


        //条件匹配，过滤
        Record result43 = filterTransformer.evaluate(record, 3, "!=", errorValue + "");
        Assert.assertEquals(result43, null);

        //条件不匹配，不过滤
        Record result44 = filterTransformer.evaluate(record, 3, "!=", value + "");
        Assert.assertEquals(result44, record);


        /**
         * bytes
         */

        //条件匹配，过滤
        Record result51 = filterTransformer.evaluate(record, 4, "==", "bazhen");
        Assert.assertEquals(result51, null);


        //条件不匹配，不过滤
        Record result52 = filterTransformer.evaluate(record, 4, "==", "errorOne");
        Assert.assertEquals(result52, record);


        //条件匹配，过滤
        Record result53 = filterTransformer.evaluate(record, 4, "!=", "errorOne");
        Assert.assertEquals(result53, null);


        //条件不匹配，不过滤
        Record result54 = filterTransformer.evaluate(record, 4, "!=", "bazhen");
        Assert.assertEquals(result54, record);

    }

    @Test
    public void testGreate() throws Exception {
        Record record = RecordProducer.produceRecord();
        FilterTransformer filterTransformer = new FilterTransformer();

        /**
         * 字符串
         */

        //条件匹配，过滤
        Record result1 = filterTransformer.evaluate(record, 1, ">", "bazhe");
        Assert.assertEquals(result1, null);


        //条件不匹配，不过滤
        Record result2 = filterTransformer.evaluate(record, 1, ">", "bazhen1");
        Assert.assertEquals(result2, record);


        //条件匹配，过滤
        Record result3 = filterTransformer.evaluate(record, 1, ">=", "bazhen");
        Assert.assertEquals(result3, null);


        //条件不匹配，不过滤
        Record result4 = filterTransformer.evaluate(record, 1, ">=", "bazhen1");
        Assert.assertEquals(result4, record);


        /**
         * long
         */

        //条件匹配，过滤
        Record result21 = filterTransformer.evaluate(record, 0, ">", "0");
        Assert.assertEquals(result21, null);

        //条件不匹配，不过滤
        Record result22 = filterTransformer.evaluate(record, 0, ">", "1");
        Assert.assertEquals(result22, record);


        //条件匹配，过滤
        Record result23 = filterTransformer.evaluate(record, 0, ">=", "1");
        Assert.assertEquals(result23, null);

        //条件不匹配，不过滤
        Record result24 = filterTransformer.evaluate(record, 0, ">=", "2");
        Assert.assertEquals(result24, record);


        /**
         * boolean
         */

        //条件匹配，过滤
        Record result31 = filterTransformer.evaluate(record, 2, ">", "false");
        Assert.assertEquals(result31, null);

        //条件不匹配，不过滤
        Record result32 = filterTransformer.evaluate(record, 2, ">", "true");
        Assert.assertEquals(result32, record);


        //条件匹配，过滤
        Record result33 = filterTransformer.evaluate(record, 2, ">=", "true");
        Assert.assertEquals(result33, null);

        //条件不匹配，不过滤
        Record result34 = filterTransformer.evaluate(record, 2, ">=", "true1");
        Assert.assertEquals(result34, record);


        /**
         * date
         */


        long value = record.getColumn(3).asLong();
        String errorValue = (value - 1) + "";

        //条件匹配，过滤
        Record result41 = filterTransformer.evaluate(record, 3, ">", errorValue + "");
        Assert.assertEquals(result41, null);

        //条件不匹配，不过滤
        Record result42 = filterTransformer.evaluate(record, 3, ">", value+"");
        Assert.assertEquals(result42, record);


        //条件匹配，过滤
        Record result43 = filterTransformer.evaluate(record, 3, ">=", value + "");
        Assert.assertEquals(result43, null);

        //条件不匹配，不过滤
        Record result44 = filterTransformer.evaluate(record, 3, ">=", (value+1) + "");
        Assert.assertEquals(result44, record);


        /**
         * bytes
         */

        //条件匹配，过滤
        Record result51 = filterTransformer.evaluate(record, 4, ">", "bazhe");
        Assert.assertEquals(result1, null);


        //条件不匹配，不过滤
        Record result52 = filterTransformer.evaluate(record, 4, ">", "bazhen1");
        Assert.assertEquals(result2, record);


        //条件匹配，过滤
        Record result53 = filterTransformer.evaluate(record, 4, ">=", "bazhen");
        Assert.assertEquals(result3, null);


        //条件不匹配，不过滤
        Record result54 = filterTransformer.evaluate(record, 4, ">=", "bazhen1");
        Assert.assertEquals(result4, record);

    }


    @Test
    public void testLess() throws Exception {
        Record record = RecordProducer.produceRecord();
        FilterTransformer filterTransformer = new FilterTransformer();

        /**
         * 字符串
         */

        //条件匹配，过滤
        Record result1 = filterTransformer.evaluate(record, 1, "<", "bazhen1");
        Assert.assertEquals(result1, null);


        //条件不匹配，不过滤
        Record result2 = filterTransformer.evaluate(record, 1, "<", "aazhen");
        Assert.assertEquals(result2, record);


        //条件匹配，过滤
        Record result3 = filterTransformer.evaluate(record, 1, "<=", "bazhen");
        Assert.assertEquals(result3, null);


        //条件不匹配，不过滤
        Record result4 = filterTransformer.evaluate(record, 1, "<=", "aazhen");
        Assert.assertEquals(result4, record);


        /**
         * long
         */

        //条件匹配，过滤
        Record result21 = filterTransformer.evaluate(record, 0, "<", "2");
        Assert.assertEquals(result21, null);

        //条件不匹配，不过滤
        Record result22 = filterTransformer.evaluate(record, 0, "<", "0");
        Assert.assertEquals(result22, record);


        //条件匹配，过滤
        Record result23 = filterTransformer.evaluate(record, 0, "<=", "1");
        Assert.assertEquals(result23, null);

        //条件不匹配，不过滤
        Record result24 = filterTransformer.evaluate(record, 0, "<=", "0");
        Assert.assertEquals(result24, record);


        /**
         * boolean
         */

        //条件匹配，过滤
        Record result31 = filterTransformer.evaluate(record, 2, "<", "true1");
        Assert.assertEquals(result31, null);

        //条件不匹配，不过滤
        Record result32 = filterTransformer.evaluate(record, 2, "<", "true");
        Assert.assertEquals(result32, record);


        //条件匹配，过滤
        Record result33 = filterTransformer.evaluate(record, 2, "<=", "true");
        Assert.assertEquals(result33, null);

        //条件不匹配，不过滤
        Record result34 = filterTransformer.evaluate(record, 2, "<=", "false");
        Assert.assertEquals(result34, record);


        /**
         * date
         */


        long value = record.getColumn(3).asLong();
        String errorValue = (value - 1) + "";

        //条件匹配，过滤
        Record result41 = filterTransformer.evaluate(record, 3, "<", (value +1)+ "");
        Assert.assertEquals(result41, null);

        //条件不匹配，不过滤
        Record result42 = filterTransformer.evaluate(record, 3, "<", value+"");
        Assert.assertEquals(result42, record);


        //条件匹配，过滤
        Record result43 = filterTransformer.evaluate(record, 3, "<=", value + "");
        Assert.assertEquals(result43, null);

        //条件不匹配，不过滤
        Record result44 = filterTransformer.evaluate(record, 3, "<=", errorValue + "");
        Assert.assertEquals(result44, record);


        /**
         * bytes
         */

        //条件匹配，过滤
        Record result51 = filterTransformer.evaluate(record, 4, "<", "bazhen1");
        Assert.assertEquals(result1, null);


        //条件不匹配，不过滤
        Record result52 = filterTransformer.evaluate(record, 4, "<", "bazhen");
        Assert.assertEquals(result2, record);


        //条件匹配，过滤
        Record result53 = filterTransformer.evaluate(record, 4, "<=", "bazhen");
        Assert.assertEquals(result3, null);


        //条件不匹配，不过滤
        Record result54 = filterTransformer.evaluate(record, 4, "<=", "bazhe");
        Assert.assertEquals(result4, record);

    }
}