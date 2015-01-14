package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.rule;

import java.math.BigDecimal;
import java.math.BigInteger;

public class PlusEvaluateRule extends EvaluateRule{

	@Override
	Long evaluate(Long A, Long B) {
		return A + B;
	}

	@Override
	Double evaluate(Long A, Double B) {
		return A + B;
	}

	@Override
	String evaluate(Long A, String B) {
		return A + B;
	}

	@Override
	 BigInteger evaluate(Long A, BigInteger B) {
		return BigInteger.valueOf(A).add(B);
	}

	@Override
	 BigDecimal evaluate(Long A, BigDecimal B) {
		return BigDecimal.valueOf(A).add(B);
	}

	@Override
	 BigInteger evaluate(BigInteger A, BigInteger B) {
		return A.add(B);
	}

	@Override
	 BigDecimal evaluate(BigInteger A, Double B) {
		return new BigDecimal(A).add(BigDecimal.valueOf(B));
	}

	@Override
	 BigInteger evaluate(BigInteger A, Long B) {
		return A.add(BigInteger.valueOf(B));
	}

	@Override
	 String evaluate(BigInteger A, String B) {
		return A + B;
	}

	@Override
	 BigDecimal evaluate(BigInteger A, BigDecimal B) {
		return B.add(new BigDecimal(A));
	}

	@Override
	 Double evaluate(Double A, Double B) {
		return A + B;
	}

	@Override
	 Double evaluate(Double A, Long B) {
		return A + B;
	}

	@Override
	 String evaluate(Double A, String B) {
		return A + B;
	}

	@Override
	 BigDecimal evaluate(Double A, BigInteger B) {
		return BigDecimal.valueOf(A).add(new BigDecimal(B));
	}

	@Override
	 BigDecimal evaluate(Double A, BigDecimal B) {
		return BigDecimal.valueOf(A).add(B);
	}

	@Override
	 String evaluate(String A, String B) {
		return A + B;
	}

	@Override
	 BigDecimal evaluate(BigDecimal A, BigDecimal B) {
		return A.add(B);
	}

	@Override
	 BigDecimal evaluate(BigDecimal A, Long B) {
		return A.add(BigDecimal.valueOf(B));
	}

	@Override
	 BigDecimal evaluate(BigDecimal A, Double B) {
		return A.add(BigDecimal.valueOf(B));
	}

	@Override
	 String evaluate(BigDecimal A, String B) {
		return A + B;
	}

	@Override
	 BigDecimal evaluate(BigDecimal A, BigInteger B) {
		return A.add(new BigDecimal(B));
	}

	@Override
	String evaluate(String A, Long B) {
		return A + B;
	}

	@Override
	String evaluate(String A, Double B) {
		return A + B;
	}

	@Override
	String evaluate(String A, BigDecimal B) {
		return A + B;
	}

	@Override
	String evaluate(String A, BigInteger B) {
		return A + B;
	}



}