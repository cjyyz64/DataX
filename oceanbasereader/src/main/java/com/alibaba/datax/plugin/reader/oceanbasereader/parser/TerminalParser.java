package com.alibaba.datax.plugin.reader.oceanbasereader.parser;

import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Scanners;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.misc.Mapper;

public class TerminalParser {

	private static final String[] OPERATORS = { "+", "-", "*", "/", "%", ">",
			"<", "=", ">=", "<=", ".", ",", "(", ")", "!=" };

	private static final String[] KEYWORDS = { "select", "from", "where",
			"as", "and", "or", "not", "is", "null", "like", "timestamp" };

	public static final Terminals TERMS = Terminals.caseInsensitive(OPERATORS,
			KEYWORDS);

	public static final Parser<?> TOKENIZER = Parsers.or(
			Terminals.DecimalLiteral.TOKENIZER,
			Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER, TERMS.tokenizer());

	static final Parser<String> TIMESTAMP = term("timestamp").next(
			Terminals.StringLiteral.PARSER);

	static <T> T parse(Parser<T> parser, String source) {
		return parser.from(TOKENIZER, Scanners.SQL_DELIMITER).parse(source);
	}

	public static Parser<?> term(String term) {
		return Mapper._(TERMS.token(term));
	}

	public static Parser<?> phrase(String phrase) {
		return Mapper._(TERMS.phrase(phrase.split("\\s")));
	}

}