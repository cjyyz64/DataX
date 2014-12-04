package com.alibaba.datax.plugin.reader.oceanbasereader.parser;

import com.alibaba.datax.plugin.reader.oceanbasereader.ast.*;
import org.codehaus.jparsec.OperatorTable;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parser.Reference;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.functors.Binary;
import org.codehaus.jparsec.functors.Unary;
import org.codehaus.jparsec.misc.Mapper;

import static com.alibaba.datax.plugin.reader.oceanbasereader.parser.TerminalParser.term;
import static com.alibaba.datax.plugin.reader.oceanbasereader.parser.TerminalParser.phrase;

public class ExpressionParser {

	private static final Parser<Expression> NULL = term("null").<Expression> retn(
			NullExpression.instance);

	private static final Parser<Expression> NUMBER = curry(NumberExpression.class)
			.sequence(Terminals.DecimalLiteral.PARSER);

	private static final Parser<Expression> STRING = curry(StringExpression.class)
			.sequence(Terminals.StringLiteral.PARSER);

	private static final Parser<Expression> TIMESTAMP = curry(TimestampExpression.class)
			.sequence(TerminalParser.TIMESTAMP);

	private static final Parser<Expression> IDENTIFIER = curry(
			IdentifierExpression.class).sequence(Terminals.Identifier.PARSER);

	static final Parser<String> ALIAS = term("as").optional().next(
			Terminals.Identifier.PARSER);
	
	private static Parser<Expression> like() {
		return curry(LikeExpression.class).sequence(
				IDENTIFIER,
				Parsers.or(term("like").retn(true),
						phrase("not like").retn(false)), STRING);
	}

	private static Parser<Expression> nullCheck() {
		return curry(BinaryExpression.class).sequence(IDENTIFIER,
				phrase("is not").retn(Operator.IS_NOT).or(phrase("is").retn(Operator.IS)),
				NULL);
	}

	private static <T> Parser<T> paren(Parser<T> parser) {
		return parser.between(term("("), term(")"));
	}

	static Parser<Expression> logical(Parser<Expression> expr) {
		Reference<Expression> ref = Parser.newReference();
		Parser<Expression> parser = new OperatorTable<Expression>()
				.infixl(binary("and", Operator.AND), 20)
				.infixl(binary("or", Operator.OR), 10)
				.build(paren(ref.lazy()).or(expr)).label("logical expression");
		ref.set(parser);
		return parser;
	}

	static Parser<Expression> condition(Parser<Expression> expr) {
		Parser<Expression> atom = compare(expr);
		return logical(atom);
	}

	private static Mapper<Expression> curry(Class<? extends Expression> clazz,
			Object... args) {
		return Mapper.curry(clazz, args);
	}

	private static Parser<Binary<Expression>> binary(String name, Operator op) {
		return term(name).next(binaryExpression(op).binary());
	}

	private static Mapper<Expression> binaryExpression(Operator op) {
		return curry(BinaryExpression.class, op);
	}

	private static Parser<Expression> compare(Parser<Expression> operand,
			String name, Operator op) {
		return curry(BinaryExpression.class).sequence(IDENTIFIER,
				term(name).retn(op), operand);
	}

	private static Parser<Expression> compare(Parser<Expression> expr) {
		return Parsers.or(compare(expr, ">", Operator.GT),
				compare(expr, ">=", Operator.GE), compare(expr, "<", Operator.LT),
				compare(expr, "<=", Operator.LE), compare(expr, "=", Operator.EQ),
				compare(expr, "!=", Operator.NE), nullCheck(), like());
	}

	static Parser<Expression> expression() {
		Parser<Expression> atom = Parsers.or(IDENTIFIER, NUMBER, TIMESTAMP, STRING);
		return arithmetic(atom).label("expression");
	}
	
	private static Parser<Unary<Expression>> unary(String name, Operator op) {
	    return term(name).next(curry(UnaryExpression.class, op).unary());
	  }
	
	private static Parser<Expression> functionCall(Parser<Expression> arg) {
		return Mapper.curry(FunctionCallExpression.class).sequence(
				Terminals.Identifier.PARSER, term("("), arg.sepBy(term(",")),
				term(")")).cast();
	}
	
	private static Parser<Expression> arithmetic(Parser<Expression> atom) {
		Reference<Expression> reference = Parser.newReference();
		Parser<Expression> operand = Parsers.or(paren(reference.lazy()),
				functionCall(reference.lazy()), atom);
		Parser<Expression> parser = new OperatorTable<Expression>()
				.infixl(binary("+", Operator.PLUS), 10)
				.infixl(binary("-", Operator.MINUS), 10)
				.infixl(binary("*", Operator.MUL), 20)
				.infixl(binary("/", Operator.DIV), 20)
				.infixl(binary("%", Operator.MOD), 20)
				.prefix(unary("-", Operator.NEG), 50).build(operand);
		reference.set(parser);
		return parser;
	}

}