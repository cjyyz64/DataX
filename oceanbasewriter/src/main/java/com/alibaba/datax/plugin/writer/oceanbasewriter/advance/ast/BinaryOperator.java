package com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.plugin.writer.oceanbasewriter.advance.TerminalParser;
import com.alibaba.datax.plugin.writer.oceanbasewriter.advance.rule.*;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Token;
import org.codehaus.jparsec.functors.Binary;
import org.codehaus.jparsec.misc.Mapper;

public enum BinaryOperator {

	PLUS("+", new PlusEvaluateRule()), Minus("-", new MinusEvaluateRule()), Multi(
			"*", new MultiEvaluateRule()), Division("/",
			new DivisionEvaluateRule()),Reminder("%", new ReminderEvaluateRule());

	private final String operator;

	private final EvaluateRule evaluateRule;

	private BinaryOperator(String operator, EvaluateRule evaluateRule) {
		this.operator = operator;
		this.evaluateRule = evaluateRule;
	}

	public Object evaluate(Record record, Expression A, Expression B) {
		return evaluateRule.evaluate(A.evaluate(record), B.evaluate(record));
	}

	public Parser<Binary<Expression>> getParser() {
		return term(this.operator).next(
				Mapper.curry(BinaryExpression.class, this).binary()).cast();
	}

	private static final Parser<Token> term(String... tokenName) {
		return Mapper._(TerminalParser.TERMS.token(tokenName)).cast();
	}

}