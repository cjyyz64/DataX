package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

public enum Operator {

	PLUS("+"){
		public String format(Expression a, Expression b) {
			return this.format(a, this, b);
		}
	}, MINUS("-"){
		public String format(Expression a, Expression b) {
			return this.format(a, this, b);
		}
	}, MUL("*"){
		public String format(Expression a, Expression b) {
			return this.format(a, this, b);
		}
	}, DIV("/"){
		public String format(Expression a, Expression b) {
			return this.format(a, this, b);
		}
	}, MOD("%"){
		public String format(Expression a, Expression b) {
			return this.format(a, this, b);
		}
	}, EQ("="), GT(">"), LT(
			"<"), GE(">="), LE("<="), NE("!="), IS("is"), IS_NOT("is not"), NEG(
			"-") {
		public String format(Expression a, Expression b) {
			return "-" + a;
		}
	},
	AND("and") {
		public String format(Expression a, Expression b) {
			return this.format(a, this, b);
		}
	},
	OR("or") {
		public String format(Expression a, Expression b) {
			return this.format(a, this, b);
		}
	};

	protected String sign;

	Operator(String sign) {
		this.sign = sign;
	}

	public String format(Expression a, Expression b) {
		return a + " " + this.sign + " " + b;
	}

	protected String format(Expression a, Operator op, Expression b){
		return "(" + a + " " + op.sign + " " + b + ")";
	}
}