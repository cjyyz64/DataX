package com.alibaba.datax.plugin.reader.oceanbasereader.visitor;

import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.BinaryExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Operator;

import java.util.Collections;
import java.util.Set;

public class PrefixScanVisitor extends Visitor<Boolean> {

	private Set<Index> indexes = Collections.emptySet();

	private boolean prefixScan = false;

	public PrefixScanVisitor(Set<Index> indexes) {
		this.indexes = Collections.unmodifiableSet(indexes);
	}

	@Override
	public void visitBinaryExpression(BinaryExpression expr) {
		if (prefixScan) return;
		expr.left.accept(this);
		expr.right.accept(this);
		if (expr.operator == Operator.EQ) {
			for (Index index : indexes) {
				for (Index.Entry entry : index) {
					if (entry.name.equalsIgnoreCase(expr.left.toString())) {
						prefixScan = true;
					}
				}
			}
		}
	}

	@Override
	public Boolean visitResult() {
		return prefixScan;
	}
}