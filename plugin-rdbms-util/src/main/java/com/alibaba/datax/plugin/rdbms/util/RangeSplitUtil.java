package com.alibaba.datax.plugin.rdbms.util;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.util.*;

public final class RangeSplitUtil {

    public static List<String> splitAndWrap(String left, String right, int expectSliceNumber,
                                            String columnName, String quote, DataBaseType dataBaseType) {
        String[] tempResult = RangeSplitUtil.doAsciiStringSplit(left, right, expectSliceNumber);
        return RangeWrapUtil.wrapRange(tempResult, columnName, quote, dataBaseType);
    }

    public static List<String> splitAndWrap(long left, long right, int expectSliceNumber, String columnName) {
        long[] tempResult = RangeSplitUtil.doLongSplit(left, right, expectSliceNumber);
        return RangeWrapUtil.wrapRange(tempResult, columnName);
    }

    public static List<String> splitAndWrap(BigInteger left, BigInteger right, int expectSliceNumber, String columnName) {
        BigInteger[] tempResult = RangeSplitUtil.doBigIntegerSplit(left, right, expectSliceNumber);
        return RangeWrapUtil.wrapRange(tempResult, columnName);
    }


    public static String[] doAsciiStringSplit(String left, String right, int expectSliceNumber) {
        int radix = 128;

        BigInteger[] tempResult = doBigIntegerSplit(stringToBigInteger(left, radix),
                stringToBigInteger(right, radix), expectSliceNumber);
        String[] result = new String[tempResult.length];

        //处理第一个字符串（因为：在转换为数字，再还原的时候，如果首字符刚好是 basic,则不知道应该添加多少个 basic）
        result[0] = left;
        result[tempResult.length - 1] = right;

        for (int i = 1, len = tempResult.length - 1; i < len; i++) {
            result[i] = bigIntegerToString(tempResult[i], radix);
        }

        return result;
    }


    public static long[] doLongSplit(long left, long right, int expectSliceNumber) {
        BigInteger[] result = doBigIntegerSplit(BigInteger.valueOf(left),
                BigInteger.valueOf(right), expectSliceNumber);
        long[] returnResult = new long[result.length];
        for (int i = 0, len = result.length; i < len; i++) {
            returnResult[i] = result[i].longValue();
        }
        return returnResult;
    }

    public static BigInteger[] doBigIntegerSplit(BigInteger left, BigInteger right, int expectSliceNumber) {
        if (expectSliceNumber < 1) {
            throw new IllegalArgumentException(String.format(
                    "expectSliceNumber can not <1. detail:expectSliceNumber=[%s].", expectSliceNumber));
        }

        if (null == left || null == right) {
            throw new IllegalArgumentException(String.format(
                    "parameter left and right can not be null. detail:left=[%s],right=[%s].", left, right));
        }

        if (left.compareTo(right) == 0) {
            return new BigInteger[]{left, right};
        } else {
            // 调整大小顺序，确保 left < right
            if (left.compareTo(right) > 0) {
                BigInteger temp = left;
                left = right;
                right = temp;
            }

            //left < right
            BigInteger endAndStartGap = right.subtract(left);

            BigInteger step = endAndStartGap.divide(BigInteger.valueOf(expectSliceNumber));
            BigInteger remainder = endAndStartGap.remainder(BigInteger.valueOf(expectSliceNumber));

            //remainder 不可能超过expectSliceNumber,所以不需要检查remainder的 Integer 的范围
            if (step.intValue() == 0) {
                expectSliceNumber = remainder.intValue();
            }

            BigInteger[] result = new BigInteger[expectSliceNumber + 1];
            result[0] = left;
            result[expectSliceNumber] = right;

            BigInteger lowerBound;
            BigInteger upperBound = left;
            for (int i = 1; i < expectSliceNumber; i++) {
                lowerBound = upperBound;
                upperBound = lowerBound.add(step);
                upperBound = upperBound.add((remainder.compareTo(BigInteger.valueOf(i)) >= 0)
                        ? BigInteger.ONE : BigInteger.ZERO);
                result[i] = upperBound;
            }

            return result;
        }
    }

    private static void checkIfBetweenRange(int value, int left, int right) {
        if (value < left || value > right) {
            throw new IllegalArgumentException(String.format("parameter can not <[%s] or >[%s].",
                    left, right));
        }
    }

    /**
     * 由于只支持 ascii 码对应字符，所以radix 范围为[1,128]
     */
    public static BigInteger stringToBigInteger(String aString, int radix) {
        if (null == aString) {
            throw new IllegalArgumentException("parameter aString can not be null.");
        }

        checkIfBetweenRange(radix, 1, 128);

        BigInteger result = BigInteger.ZERO;
        BigInteger radixBigInteger = BigInteger.valueOf(radix);

        int tempChar = -1;
        int k = 0;

        for (int i = aString.length() - 1; i >= 0; i--) {
            tempChar = aString.charAt(i);
            if (tempChar >= 128) {
                throw new IllegalArgumentException("parameter aString can not contains Non-Ascii character.");
            }
            result = result.add(BigInteger.valueOf(tempChar).multiply(radixBigInteger.pow(k)));
            k++;
        }

        return result;
    }

    /**
     * 把BigInteger 转换为 String.注意：radix 和 basic 范围都为[1,128], radix + basic 的范围也必须在[1,128].
     */
    private static String bigIntegerToString(BigInteger bigInteger, int radix) {
        if (null == bigInteger) {
            throw new IllegalArgumentException("parameter bigInteger can not be null.");
        }

        checkIfBetweenRange(radix, 1, 128);

        StringBuilder resultStringBuilder = new StringBuilder();

        List<Integer> list = new ArrayList<Integer>();
        BigInteger radixBigInteger = BigInteger.valueOf(radix);
        BigInteger currentValue = bigInteger;

        BigInteger quotient = currentValue.divide(radixBigInteger);
        while (quotient.compareTo(BigInteger.ZERO) > 0) {
            list.add(currentValue.remainder(radixBigInteger).intValue());
            currentValue = currentValue.divide(radixBigInteger);
            quotient = currentValue;
        }
        Collections.reverse(list);

        if (list.isEmpty()) {
            list.add(0, bigInteger.remainder(radixBigInteger).intValue());
        }

        Map<Integer, Character> map = new HashMap<Integer, Character>();
        for (int i = 0; i < radix; i++) {
            map.put(i, (char) (i));
        }

//        String msg = String.format("%s 转为 %s 进制，结果为：%s", bigInteger.longValue(), radix, list);
//        System.out.println(msg);

        for (int i = 0, len = list.size(); i < len; i++) {
            resultStringBuilder.append(map.get(list.get(i)));
        }

        return resultStringBuilder.toString();
    }

    /**
     * 获取字符串中的最小字符和最大字符（依据 ascii 进行判断）.要求字符串必须非空，并且为 ascii 字符串.
     * 返回的Pair，left=最小字符，right=最大字符.
     */
    public static Pair<Character, Character> getMinAndMaxCharacter(String aString) {
        if (!isPureAscii(aString)) {
            throw new IllegalArgumentException(String.format(
                    "parameter aString not pure ascii. detail:aString=[%s].", aString));
        }

        char min = aString.charAt(0);
        char max = min;

        char temp = 0;
        for (int i = 1, len = aString.length(); i < len; i++) {
            temp = aString.charAt(i);
            min = min < temp ? min : temp;
            max = max > temp ? max : temp;
        }

        return new ImmutablePair<Character, Character>(min, max);
    }

    private static boolean isPureAscii(String aString) {
        if (null == aString) {
            return false;
        }

        for (int i = 0, len = aString.length(); i < len; i++) {
            char ch = aString.charAt(i);
            if (ch >= 127 || ch < 0) {
                return false;
            }
        }
        return true;
    }

}
