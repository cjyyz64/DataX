package com.alibaba.datax.plugin.reader.oceanbasereader.visitor;

import com.alibaba.datax.plugin.reader.oceanbasereader.ast.BinaryExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Expression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Operator;

public class ModifyConditionVisitor extends Visitor<Void> {

	private Expression expr;
	private String field;

	public ModifyConditionVisitor(String field, final String condition) {
		this.expr = new Expression() {
			@Override
			public String toString() {
				return condition;
			}
		};
		this.field = field;
	}

	@Override
	public void visitBinaryExpression(BinaryExpression expr) {
		expr.left.accept(this);
		expr.right.accept(this);
		if(columnNameSame(expr) && meetOperator(expr.operator)){
			expr.operator = Operator.GT;
			expr.right = this.expr;
		}
	}

	@Override
	public Void visitResult() {
		throw new UnsupportedOperationException(
				"ModifyConditionVisitor not support visitResult()");
	}

	private boolean columnNameSame(BinaryExpression expr){
		return field.equalsIgnoreCase(expr.left.toString());
	}
	
	private boolean meetOperator(Operator operator){
		return operator == Operator.GE || operator == Operator.GT;
	}
}