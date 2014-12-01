package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.rule;

import java.math.BigDecimal;
import java.math.BigInteger;

public class MultiEvaluateRule extends EvaluateRule{

	@Override
	Long evaluate(Long A, Long B) {
		return A * B;
	}

	@Override
	Double evaluate(Long A, Double B) {
		return A * B;
	}

	@Override
	String evaluate(Long A, String B) {
		throw new UnsupportedOperationException(String.format("String[%s] and String[%s] can not multiply", A, B));
	}

	@Override
	BigInteger evaluate(Long A, BigInteger B) {
		return BigInteger.valueOf(A).multiply(B);
	}

	@Override
	BigDecimal evaluate(Long A, BigDecimal B) {
		return BigDecimal.valueOf(A).multiply(B);
	}

	@Override
	BigInteger evaluate(BigInteger A, BigInteger B) {
		return A.multiply(B);
	}

	@Override
	BigDecimal evaluate(BigInteger A, Double B) {
		return new BigDecimal(A).multiply(BigDecimal.valueOf(B));
	}

	@Override
	BigInteger evaluate(BigInteger A, Long B) {
		return A.multiply(BigInteger.valueOf(B));
	}

	@Override
	String evaluate(BigInteger A, String B) {
		throw new UnsupportedOperationException(String.format("BigInteger[%s] and String[%s] can not multiply", A, B));
	}

	@Override
	BigDecimal evaluate(BigInteger A, BigDecimal B) {
		return new BigDecimal(A).multiply(B);
	}

	@Override
	Double evaluate(Double A, Double B) {
		return A * B;
	}

	@Override
	Double evaluate(Double A, Long B) {
		return A * B;
	}

	@Override
	String evaluate(Double A, String B) {
		throw new UnsupportedOperationException(String.format("Double[%s] and String[%s] can not multiply", A, B));
	}

	@Override
	BigDecimal evaluate(Double A, BigInteger B) {
		return BigDecimal.valueOf(A).multiply(new BigDecimal(B));
	}

	@Override
	BigDecimal evaluate(Double A, BigDecimal B) {
		return BigDecimal.valueOf(A).multiply(B);
	}

	@Override
	String evaluate(String A, Long B) {
		throw new UnsupportedOperationException(String.format("String[%s] and Long[%s] can not multiply", A, B));
	}

	@Override
	String evaluate(String A, Double B) {
		throw new UnsupportedOperationException(String.format("String[%s] and Double[%s] can not multiply", A, B));
	}

	@Override
	String evaluate(String A, BigDecimal B) {
		throw new UnsupportedOperationException(String.format("String[%s] and BigDecimal[%s] can not multiply", A, B));
	}

	@Override
	String evaluate(String A, String B) {
		throw new UnsupportedOperationException(String.format("String[%s] and String[%s] can not multiply", A, B));
	}

	@Override
	String evaluate(String A, BigInteger B) {
		throw new UnsupportedOperationException(String.format("String[%s] and BigInteger[%s] can not multiply", A, B));
	}

	@Override
	BigDecimal evaluate(BigDecimal A, BigDecimal B) {
		return A.multiply(B);
	}

	@Override
	BigDecimal evaluate(BigDecimal A, Long B) {
		return A.multiply(BigDecimal.valueOf(B));
	}

	@Override
	BigDecimal evaluate(BigDecimal A, Double B) {
		return A.multiply(BigDecimal.valueOf(B));
	}

	@Override
	String evaluate(BigDecimal A, String B) {
		throw new UnsupportedOperationException(String.format("BigDecimal[%s] and String[%s] can not multiply", A, B));
	}

	@Override
	BigDecimal evaluate(BigDecimal A, BigInteger B) {
		return A.multiply(new BigDecimal(B));
	}



}