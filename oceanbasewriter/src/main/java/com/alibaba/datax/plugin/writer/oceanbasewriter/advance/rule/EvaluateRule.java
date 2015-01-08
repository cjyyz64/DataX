package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.rule;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class EvaluateRule {

	//for Long
	abstract Long evaluate(Long A,Long B);
	abstract Double evaluate(Long A,Double B);
	abstract String evaluate(Long A,String B);
	abstract BigInteger evaluate(Long A,BigInteger B);
	abstract BigDecimal evaluate(Long A,BigDecimal B);
	
	//for BigInteger
	abstract BigInteger evaluate(BigInteger A,BigInteger B);
	abstract BigDecimal evaluate(BigInteger A,Double B);
	abstract BigInteger evaluate(BigInteger A,Long B);
	abstract String evaluate(BigInteger A,String B);
	abstract BigDecimal evaluate(BigInteger A,BigDecimal B);
	
	//for Double
	abstract Double evaluate(Double A,Double B);
	abstract Double evaluate(Double A,Long B);
	abstract String evaluate(Double A,String B);
	abstract BigDecimal evaluate(Double A,BigInteger B);
	abstract BigDecimal evaluate(Double A,BigDecimal B);

	//for String
	abstract String evaluate(String A,Long B);
	abstract String evaluate(String A,Double B);
	abstract String evaluate(String A,BigDecimal B);
	abstract String evaluate(String A,String B);
	abstract String evaluate(String A,BigInteger B);
	
	//for BigDecimal
	abstract BigDecimal evaluate(BigDecimal A,BigDecimal B);
	abstract BigDecimal evaluate(BigDecimal A,Long B);	
	abstract BigDecimal evaluate(BigDecimal A,Double B);
	abstract String evaluate(BigDecimal A,String B);
	abstract BigDecimal evaluate(BigDecimal A,BigInteger B);
	
	public Object evaluate(Object A, Object B){
		if(A == null || B == null){
			throw new UnsupportedOperationException("operand not support null");
		}
		try {
			Method method = this.getClass().getDeclaredMethod("evaluate", A.getClass(),B.getClass());
			return method.invoke(this, A, B);
		} catch (Exception e) {
			throw new UnsupportedOperationException(String.format("unsupport %s + %s", A.getClass(), B.getClass()));
		}
	};

}