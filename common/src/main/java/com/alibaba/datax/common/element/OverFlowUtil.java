package com.alibaba.datax.common.element;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

public final class OverFlowUtil {
	public static final BigInteger MAX_LONG = BigInteger
			.valueOf(Long.MAX_VALUE);

	public static final BigInteger MIN_LONG = BigInteger
			.valueOf(Long.MIN_VALUE);

	public static final BigDecimal MIN_DOUBLE_POSITIVE = new BigDecimal(
			String.valueOf(Double.MIN_VALUE));

	public static final BigDecimal MAX_DOUBLE_POSITIVE = new BigDecimal(
			String.valueOf(Double.MAX_VALUE));

	public static boolean isLongOverflow(final BigInteger integer) {
		return (integer.compareTo(OverFlowUtil.MAX_LONG) > 0 || integer
				.compareTo(OverFlowUtil.MIN_LONG) < 0);

	}

	public static void validateLongNotOverFlow(final BigInteger integer) {
		boolean isOverFlow = OverFlowUtil.isLongOverflow(integer);

		if (isOverFlow) {
			throw new DataXException(CommonErrorCode.CONVERT_OVER_FLOW,
					String.format("Value [%s] convert to long overflow !",
							integer.toString()));
		}
	}

	public static boolean isDoubleOverFlow(final BigDecimal decimal) {
		if (decimal.signum() == 0) {
			return false;
		}

		BigDecimal newDecimal = decimal;
		boolean isPositive = decimal.signum() == 1;
		if (!isPositive) {
			newDecimal = decimal.negate();
		}

		return (newDecimal.compareTo(MIN_DOUBLE_POSITIVE) < 0 || newDecimal
				.compareTo(MAX_DOUBLE_POSITIVE) > 0);
	}

	public static void validateDoubleNotOverFlow(final BigDecimal decimal) {
		boolean isOverFlow = OverFlowUtil.isDoubleOverFlow(decimal);
		if (isOverFlow) {
			throw new DataXException(CommonErrorCode.CONVERT_OVER_FLOW,
					String.format("Value [%s] convert to double overflow !",
							decimal.toPlainString()));
		}
	}
}
