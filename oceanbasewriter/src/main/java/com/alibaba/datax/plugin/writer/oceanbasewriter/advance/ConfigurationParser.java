package com.alibaba.datax.plugin.writer.oceanbasewriter.advance;

import com.alibaba.datax.plugin.writer.oceanbasewriter.advance.ast.*;
import org.codehaus.jparsec.*;
import org.codehaus.jparsec.misc.Mapper;

public class ConfigurationParser {

	static final Parser<Expression> STRING_PARSER = Mapper
			.curry(StringExpression.class)
			.sequence(Terminals.StringLiteral.PARSER).cast();

	static final Parser<Expression> PREDEFINED_VARIABLE_PARSER = TerminalParser.PredefinedVariable.PARSER
			.cast();

	static final Parser<TimestampExpression> DATE_FORMAT_PARSER;
	static {
		Parser<Expression> target = STRING_PARSER
				.or(PREDEFINED_VARIABLE_PARSER);
		Parser<TimestampExpression> defaultParser = Mapper.curry(
				TimestampExpression.class).sequence(term("("),
				term("Timestamp").retn("yyyy-MM-dd HH:mm:ss"), term(")"),
				target);
		Parser<TimestampExpression> formatParser = Mapper.curry(
				TimestampExpression.class).sequence(term("("),
				term("Timestamp").next(Terminals.StringLiteral.PARSER),
				term(")"), target);
		DATE_FORMAT_PARSER = defaultParser.or(formatParser);
	}

	static final Parser<ValueExpression> NUMBER_PARSER = Mapper.curry(
			ValueExpression.class).sequence(Terminals.DecimalLiteral.PARSER);

	static final Parser<BigDecimalExpression> BIGDECIMAL_PARSER = Mapper.curry(
			BigDecimalExpression.class).sequence(term("("), term("BigDecimal"),
			term(")"), Terminals.DecimalLiteral.PARSER);

	static final Parser<BigIntegerExpression> BIGINTEGER_PARSER = Mapper.curry(
			BigIntegerExpression.class).sequence(term("("), term("BigInteger"),
			term(")"), Terminals.DecimalLiteral.PARSER);

	private static final Parser<Token> term(String... tokenName) {
		return Mapper._(TerminalParser.TERMS.token(tokenName)).cast();
	}

	private static final Parser<Expression> castOrExpression(
			Parser<Expression> expr) {
		return Mapper
				.curry(CastExpression.class)
				.sequence(term("("), TerminalParser.KEYWORDS_PARSER, term(")"),
						ATOM.or(expr)).<Expression> cast().or(paren(expr));
	}

	private static Parser<Expression> paren(Parser<Expression> parser) {
		return parser.between(term("("), term(")"));
	}

	static final Parser<Expression> ATOM = Parsers.or(DATE_FORMAT_PARSER,
			STRING_PARSER, NUMBER_PARSER, BIGDECIMAL_PARSER, BIGINTEGER_PARSER,
			PREDEFINED_VARIABLE_PARSER);

	static Parser<Expression> expression(Parser<Expression> atom) {
		Parser.Reference<Expression> ref = Parser.newReference();
		Parser<Expression> lazy = ref.lazy();
		atom = Parsers.or(atom, castOrExpression(lazy));
		Parser<Expression> parser = new OperatorTable<Expression>()
				.infixl(BinaryOperator.PLUS.getParser(), 10)
				.infixl(BinaryOperator.Minus.getParser(), 10)
				.infixl(BinaryOperator.Multi.getParser(), 10)
				.infixl(BinaryOperator.Division.getParser(), 10)
				.infixl(BinaryOperator.Reminder.getParser(), 10).build(atom);
		ref.set(parser);
		return parser;
	}

	private static Parser<ConfigurationItem> PARSER = Mapper
			.curry(ConfigurationItem.class)
			.sequence(Terminals.Identifier.PARSER, term("="), expression(ATOM))
			.from(TerminalParser.TOKENIZER, Scanners.JAVA_DELIMITER);

	public static ConfigurationItem parse(String config) {
		return PARSER.parse(config);
	}

}