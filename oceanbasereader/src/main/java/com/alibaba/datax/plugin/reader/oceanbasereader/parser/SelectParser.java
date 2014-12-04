package com.alibaba.datax.plugin.reader.oceanbasereader.parser;

import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Expression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.ProjectionExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.SelectExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.WildCardlExpression;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Scanners;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.misc.Mapper;

import java.util.List;

import static com.alibaba.datax.plugin.reader.oceanbasereader.parser.TerminalParser.TOKENIZER;
import static com.alibaba.datax.plugin.reader.oceanbasereader.parser.TerminalParser.term;

public class SelectParser {

	private static final Parser<Expression> WILDCARD = term("*")
			.<Expression> retn(WildCardlExpression.instance);

	private static Parser<List<Expression>> projectionClause(Parser<Expression> expr){
		return Mapper.curry(ProjectionExpression.class)
				.sequence(expr, ExpressionParser.ALIAS.optional())
				.sepBy1(term(",")).cast();
	}
	
	private static Parser<String> fromClause() {
		return term("from").next(Terminals.Identifier.PARSER);
	}

	private static Parser<Expression> whereClause(Parser<Expression> cond) {
		return term("where").next(cond);
	}

	
	private static Parser<SelectExpression> select(Parser<Expression> projection,
			Parser<Expression> cond) {
		return curry(SelectExpression.class).sequence(term("select"),
				projectionClause(projection), fromClause(),
				whereClause(cond).optional());
	}

	private static Mapper<SelectExpression> curry(
			Class<? extends SelectExpression> clazz, Object... args) {
		return Mapper.curry(clazz, args);
	}

	private static Parser<SelectExpression> select() {
		Parser<Expression> expr = ExpressionParser.expression();
		Parser<Expression> cond = ExpressionParser.condition(expr);
		Parser<SelectExpression> select = select(expr.or(WILDCARD), cond);
		return select;
	}

	public static SelectExpression parse(String source) {
		return select().from(TOKENIZER, Scanners.SQL_DELIMITER).parse(source);
	}

}