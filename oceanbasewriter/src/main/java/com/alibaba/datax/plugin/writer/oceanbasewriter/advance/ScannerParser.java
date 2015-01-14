package com.alibaba.datax.plugin.writer.oceanbasewriter.advance;

import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Scanners;
import org.codehaus.jparsec.pattern.Patterns;

public class ScannerParser {

	static final Parser<String> PREDEFINED_VARIABLE = Scanners.pattern(
			Patterns.isChar('F').next(Patterns.INTEGER), "predifined variable")
			.source();
}