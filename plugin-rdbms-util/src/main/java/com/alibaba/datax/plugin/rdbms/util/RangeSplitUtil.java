package com.alibaba.datax.plugin.rdbms.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.util.*;

public final class RangeSplitUtil {

    public static List<String> getAsciiStringSplitAndWarappedResult(String left, String right, int expectSliceNumber,
                                                                    String columnName, String quote) {
        String[] tempResult = doAsciiStringSplit(left, right, expectSliceNumber);
        return doConditionWrap(tempResult, columnName, quote);
    }

    public static List<String> doConditionWrap(long[] rangeResult, String columnName) {
        String[] rangeStr = new String[rangeResult.length];
        for (int i = 0, len = rangeResult.length; i < len; i++) {
            rangeStr[i] = String.valueOf(rangeResult[i]);
        }
        return doConditionWrap(rangeStr, columnName, "");
    }

    public static List<String> doConditionWrap(BigInteger[] rangeResult, String columnName) {
        String[] rangeStr = new String[rangeResult.length];
        for (int i = 0, len = rangeResult.length; i < len; i++) {
            rangeStr[i] = rangeResult[i].toString();
        }
        return doConditionWrap(rangeStr, columnName, "");
    }

    //TODO 目前是按照 mysql的 sql 特征 进行 quote的
    public static List<String> doConditionWrap(String[] rangeResult, String columnName, String quote) {
        if (null == rangeResult || rangeResult.length < 2) {
            throw new IllegalArgumentException(String.format(
                    "Parameter rangeResult can not be null and its length can not <2. detail:rangeResult=[%s].",
                    StringUtils.join(rangeResult, ",")));
        }

        List<String> result = new ArrayList<String>();
        if (2 == rangeResult.length) {
            result.add(String.format(" %s%s%s >= %s AND %s <= %s%s%s ", quote, quoteMysqlConstantValue(rangeResult[0]), quote,
                    columnName, columnName, quote, quoteMysqlConstantValue(rangeResult[1]), quote));
            return result;
        } else {
            for (int i = 0, len = rangeResult.length - 2; i < len; i++) {
                result.add(String.format(" %s%s%s >= %s AND %s < %s%s%s ", quote, quoteMysqlConstantValue(rangeResult[i]), quote,
                        columnName, columnName, quote, quoteMysqlConstantValue(rangeResult[i + 1]), quote));
            }

            result.add(String.format(" %s%s%s >= %s AND %s <= %s%s%s ", quote, quoteMysqlConstantValue(rangeResult[rangeResult.length - 2]),
                    quote, columnName, columnName, quote, quoteMysqlConstantValue(rangeResult[rangeResult.length - 1]), quote));
            return result;
        }
    }

    private static String quoteMysqlConstantValue(String aString) {
        return aString.replace("'", "''").replace("\\", "\\\\");
    }

    public static String[] doAsciiStringSplit(String left, String right, int expectSliceNumber) {
        Pair<Integer, Integer> leftMinMax = getMinAndMaxCharacter(left);
        Pair<Integer, Integer> rightMinMax = getMinAndMaxCharacter(right);

        List<Integer> aList = new ArrayList<Integer>();
        aList.add(leftMinMax.getLeft());
        aList.add(leftMinMax.getRight());

        aList.add(rightMinMax.getLeft());
        aList.add(rightMinMax.getRight());

        int basic = Collections.min(aList).intValue();
        int radix = Collections.max(aList).intValue() - basic + 1;

        BigInteger[] result = doBigIntegerSplit(stringToBigInteger(left, radix, basic),
                stringToBigInteger(right, radix, basic), expectSliceNumber);
        String[] returnResult = new String[result.length];

        //处理第一个字符串（因为：在转换为数字，再还原的时候，如果首字符刚好是 basic,则不知道应该添加多少个 basic）
        returnResult[0] = left;

        for (int i = 1, len = result.length; i < len; i++) {
            returnResult[i] = bigIntegerToString(result[i], radix, basic);
        }


        return returnResult;
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
            if (right.subtract(left).compareTo(BigInteger.valueOf(expectSliceNumber)) <= 0) {
                BigInteger tempValue = right.subtract(left);
                if (tempValue.intValue() > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("exceed range.");
                }

                expectSliceNumber = tempValue.intValue();
            }

            BigInteger endAndStartGap = right.subtract(left);

            BigInteger step = endAndStartGap.divide(BigInteger.valueOf(expectSliceNumber));
            BigInteger remainder = endAndStartGap.remainder(BigInteger.valueOf(expectSliceNumber));

            if (step.intValue() == 0) {
                expectSliceNumber = remainder.intValue();
            }

            BigInteger[] splittedResult = new BigInteger[expectSliceNumber + 1];
            splittedResult[0] = left;
            splittedResult[expectSliceNumber] = right;

            BigInteger lowerBound;
            BigInteger upperBound = left;
            for (int i = 1; i < expectSliceNumber; i++) {
                lowerBound = upperBound;
                upperBound = lowerBound.add(step);
                upperBound = upperBound.add((remainder.compareTo(BigInteger.valueOf(i)) >= 0)
                        ? BigInteger.ONE : BigInteger.ZERO);
                splittedResult[i] = upperBound;
            }

            return splittedResult;
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
    public static BigInteger stringToBigInteger(String aString, int radix, int basic) {
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
            tempChar -= basic;
            if (tempChar < 0) {
                throw new IllegalArgumentException("parameter basic can not bigger than aString character.");
            }
            result = result.add(BigInteger.valueOf(tempChar).multiply(radixBigInteger.pow(k)));
            k++;
        }

        return result;
    }

    public static String bigIntegerToString(BigInteger bigInteger, int radix, int basic) {

        Map<Integer, Character> map = new HashMap<Integer, Character>();

        for (int i = 0; i < radix; i++) {
            map.put(i, (char) (i + basic));
        }

        List<Integer> list = new ArrayList<Integer>();
        StringBuilder stringBuilder = new StringBuilder();
        BigInteger currentValue = bigInteger;
        BigInteger radixBigInteger = BigInteger.valueOf(radix);

        BigInteger sang = currentValue.divide(radixBigInteger);
        while (sang.compareTo(BigInteger.ZERO) > 0) {
            list.add(currentValue.remainder(radixBigInteger).intValue());
            currentValue = currentValue.divide(radixBigInteger);
            sang = currentValue;
        }
        Collections.reverse(list);

        if (list.isEmpty()) {
            list.add(0, currentValue.remainder(radixBigInteger).intValue());
        }

//        String msg = String.format("%s 转为 %s 进制，结果为：%s", bigInteger.longValue(), radix, list);
//        System.out.println(msg);

        for (int i = 0, len = list.size(); i < len; i++) {
            stringBuilder.append(map.get(list.get(i)));
        }

        return stringBuilder.toString();
    }

    public static Pair<Integer, Integer> getMinAndMaxCharacter(String aString) {
        int min = aString.charAt(0);
        int max = min;

        int temp = 0;
        for (int i = 1, len = aString.length(); i < len; i++) {
            temp = aString.charAt(i);
            min = min < temp ? min : temp;
            max = max > temp ? max : temp;
        }

        return new ImmutablePair<Integer, Integer>(min, max);
    }

}
