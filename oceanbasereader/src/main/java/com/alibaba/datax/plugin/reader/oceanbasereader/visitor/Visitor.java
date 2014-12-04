package com.alibaba.datax.plugin.reader.oceanbasereader.visitor;

import com.alibaba.datax.plugin.reader.oceanbasereader.ast.BinaryExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Expression;

public abstract class Visitor<T> {

	public abstract void visitBinaryExpression(BinaryExpression expr);
	
	public void visit(Expression expr){}
	
	public abstract T visitResult();


}