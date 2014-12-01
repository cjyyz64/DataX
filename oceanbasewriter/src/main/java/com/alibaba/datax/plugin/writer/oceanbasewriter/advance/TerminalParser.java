package com.alibaba.datax.plugin.writer.oceanbasewriter.advance;

import com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast.PredefinedExpression;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.Token;
import org.codehaus.jparsec.functors.Map;
import org.codehaus.jparsec.misc.Mapper;

public class TerminalParser {

	public static final class PredefinedVariable {

		private PredefinedVariable() {
		}

		public static final Parser<?> TOKENIZER = Mapper.curry(
				PredefinedExpression.class).sequence(
				ScannerParser.PREDEFINED_VARIABLE);
		public static final Parser<PredefinedExpression> PARSER = Parsers
				.tokenType(PredefinedExpression.class, "predefined variable");
	}

	private static final String[] OPERATORS = { "+", "-", "*", "/", "%", "(",
			")", "|", ";", "=" };
	private static final String[] KEYWORDS = { "Double", "Long", "Timestamp",
			"String", "BigInteger", "BigDecimal" };

	public static final Terminals TERMS = Terminals.caseSensitive(OPERATORS,
			KEYWORDS);

	static final Parser<String> KEYWORDS_PARSER = TERMS.token("Double", "Long",
			"String", "BigInteger", "BigDecimal").map(new Map<Token, String>() {

		@Override
		public String map(Token from) {
			return from.toString();
		}
	});

	public static final Parser<?> TOKENIZER = Parsers.or(
			Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER,
			PredefinedVariable.TOKENIZER, Terminals.DecimalLiteral.TOKENIZER,
			TERMS.tokenizer());// 关键字的parser跟预定义变量parser顺序不可以调换


}