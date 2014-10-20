package com.alibaba.datax.plugin.rdbms.util;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class RangeSplitUtilTest {

    @Test
    public void testLong() {
        long left = 8L;
        long right = 301L;
        int expectSliceNumber = 93;
        doTest(left, right, expectSliceNumber);

        for (int i = 1; i < right * 20; i++) {
            doTest(left, right, i);
        }

        System.out.println(" 测试随机值...");
        int testTimes = 200;
        for (int i = 0; i < testTimes; i++) {
            left = getRandomLong();
            right = getRandomLong();
            expectSliceNumber = getRandomInteger();
            doTest(left, right, expectSliceNumber);
        }

    }


    @Test
    public void testGetMinAndMaxCharacter() {
        Pair<Character, Character> result = RangeSplitUtil.getMinAndMaxCharacter("abc%^&");
        Assert.assertEquals('%', result.getLeft().charValue());
        Assert.assertEquals('c', result.getRight().charValue());

        result = RangeSplitUtil.getMinAndMaxCharacter("\tAabcZx");
        Assert.assertEquals('\t', result.getLeft().charValue());
        Assert.assertEquals('x', result.getRight().charValue());
    }


    //TODO 自动化测试
    @Test
    public void testDoAsciiStringSplit() {
//        String left = "adde";
//        String right = "xyz";
//        int expectSliceNumber = 4;
        String left = "a中";
        String right = "baa";
        int expectSliceNumber = 16;

        String[] result = RangeSplitUtil.doAsciiStringSplit(left, right, expectSliceNumber);
        System.out.println(ToStringBuilder.reflectionToString(result, ToStringStyle.SIMPLE_STYLE));
        System.out.println(RangeSplitUtil.splitAndWrap(left, right, expectSliceNumber, "id", "'", DataBaseType.MySql));

    }

    private long getRandomLong() {
        Random r = new Random();
        return r.nextLong();
    }

    private int getRandomInteger() {
        Random r = new Random();
        return Math.abs(r.nextInt(1000) + 1);
    }

    private void doTest(long left, long right, int expectSliceNumber) {
        long[] result = RangeSplitUtil.doLongSplit(left, right, expectSliceNumber);

        System.out.println(String.format("left:[%s],right:[%s],expectSliceNumber:[%s]====> splitResult:[\n%s\n].\n",
                left, right, expectSliceNumber, ToStringBuilder.reflectionToString(result, ToStringStyle.SIMPLE_STYLE)));

        Assert.assertTrue(doCheck(result, left, right, Math.abs(right - left) >
                expectSliceNumber ? expectSliceNumber : -1));
    }


    @SuppressWarnings("unused")
	private boolean doCheck(long[] result, long left, long right) {
        return doCheck(result, left, right, -1);
    }

    private boolean doCheck(long[] result, long left,
                            long right, int expectSliceNumber) {
        if (null == result) {
            throw new IllegalArgumentException("parameter result can not be null.");
        }

        // 调整大小顺序，确保 left<right
        if (left > right) {
            long temp = left;
            left = right;
            right = temp;
        }

        //为了方法共用，expectSliceNumber == -1 表示不对切分份数进行校验.
        boolean skipSliceNumberCheck = expectSliceNumber == -1;
        if (skipSliceNumberCheck || expectSliceNumber == result.length - 1) {
            boolean leftCheckOk = left == result[0];
            boolean rightCheckOk = right == result[result.length - 1];

            if (leftCheckOk && rightCheckOk) {
                for (int i = 0, len = result.length; i < len - 1; i++) {
                    if (result[i] > result[i + 1]) {
                        return false;
                    }
                }
                return true;
            }
        }

        return false;
    }
}
