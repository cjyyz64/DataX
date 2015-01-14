package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class CastExpression implements Expression {

	private final CastRule rule;
	private final Expression expression;
	private static final Map<String, CastRule> CLAZZ_MAP = new HashMap<String, CastRule>();

	static {
		CLAZZ_MAP.put("Double", CastRule.DoubleRule);
		CLAZZ_MAP.put("Long", CastRule.LongRule);
		CLAZZ_MAP.put("String", CastRule.StringRule);
		CLAZZ_MAP.put("BigDecimal", CastRule.BigDecimalRule);
		CLAZZ_MAP.put("BigInteger", CastRule.BigIntegerRule);
	}

	public CastExpression(String type, Expression expression) {
		this.rule = CLAZZ_MAP.get(type);
		if (rule == null) {
			throw new IllegalArgumentException(
					String.format(
							"unsupport type[%s] cast. Tip: only support (Double)(Long)(Timestamp)(String)(BigDecimal)(BigInteger). Case sensitive.",
							type));
		}
		this.expression = expression;
	}

	@Override
	public Object evaluate(Record record) {
		Object value = expression.evaluate(record);
		if(value == null){
			throw new UnsupportedOperationException("Cast not support null");
		}
		try {
			Method method = rule.getClass().getDeclaredMethod("cast", value.getClass());
			return method.invoke(rule, value);
		} catch (Exception e) {
			throw new UnsupportedOperationException(String.format(
					"cast %s type error with %s", value.getClass(), rule), e);
		}
	}

	private enum CastRule {
		StringRule {
			@Override
			Object cast(Double value) {
				return value.toString();
			}

			@Override
			Object cast(Long value) {
				return value.toString();
			}

			@Override
			Object cast(Timestamp value) {
				return value.toString();
			}

			@Override
			Object cast(String value) {
				return value;
			}

			@Override
			Object cast(BigDecimal value) {
				return value.toString();
			}

			@Override
			Object cast(BigInteger value) {
				return value.toString();
			}
		},
		DoubleRule {
			@Override
			Object cast(Double value) {
				return value;
			}

			@Override
			Object cast(Long value) {
				return Double.valueOf(value);
			}

			@Override
			Object cast(Timestamp value) {
				throw new UnsupportedOperationException(String.format(
						"can not cast %s to %s", Timestamp.class, Double.class));
			}

			@Override
			Object cast(String value) {
				return Double.valueOf(value);
			}

			@Override
			Object cast(BigDecimal value) {
				return value.doubleValue();
			}

			@Override
			Object cast(BigInteger value) {
				return value.doubleValue();
			}
		},
		LongRule {
			@Override
			Object cast(Double value) {
				throw new UnsupportedOperationException(String.format(
						"can not cast %s to %s", Double.class, Long.class));
			}

			@Override
			Object cast(Long value) {
				return value;
			}

			@Override
			Object cast(Timestamp value) {
				throw new UnsupportedOperationException(String.format(
						"can not cast %s to %s", Timestamp.class, Long.class));
			}

			@Override
			Object cast(String value) {
				return Long.valueOf(value);
			}

			@Override
			Object cast(BigDecimal value) {
				return value.doubleValue();
			}

			@Override
			Object cast(BigInteger value) {
				return value.doubleValue();
			}
		},
		BigDecimalRule {
			@Override
			Object cast(Double value) {
				return BigDecimal.valueOf(value);
			}

			@Override
			Object cast(Long value) {
				return BigDecimal.valueOf(value);
			}

			@Override
			Object cast(Timestamp value) {
				throw new UnsupportedOperationException(String.format(
						"can not cast %s to %s", Timestamp.class, BigDecimal.class));
			}

			@Override
			Object cast(String value) {
				return new BigDecimal(value);
			}

			@Override
			Object cast(BigDecimal value) {
				return value;
			}

			@Override
			Object cast(BigInteger value) {
				return new BigDecimal(value);
			}
		},
		BigIntegerRule {
			@Override
			Object cast(Double value) {
				throw new UnsupportedOperationException(
						String.format("can not cast %s to %s", Double.class,
								BigInteger.class));
			}

			@Override
			Object cast(Long value) {
				return BigInteger.valueOf(value);
			}

			@Override
			Object cast(Timestamp value) {
				throw new UnsupportedOperationException(String.format(
						"can not cast %s to %s", Timestamp.class, BigInteger.class));
			}

			@Override
			Object cast(String value) {
				return new BigInteger(value);
			}

			@Override
			Object cast(BigDecimal value) {
				throw new UnsupportedOperationException(String.format(
						"can not cast %s to %s", BigDecimal.class,
						BigInteger.class));
			}

			@Override
			Object cast(BigInteger value) {
				return value;
			}
		};

		abstract Object cast(Double value);
		abstract Object cast(Long value);
		abstract Object cast(Timestamp value);
		abstract Object cast(String value);
		abstract Object cast(BigDecimal value);
		abstract Object cast(BigInteger value);
	}

}