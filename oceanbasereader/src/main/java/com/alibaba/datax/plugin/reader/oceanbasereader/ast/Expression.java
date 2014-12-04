package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

import com.alibaba.datax.plugin.reader.oceanbasereader.visitor.Visitor;

public abstract class Expression {

	public void accept(Visitor<?> visitor) {
		visitor.visit(this);
	}
}