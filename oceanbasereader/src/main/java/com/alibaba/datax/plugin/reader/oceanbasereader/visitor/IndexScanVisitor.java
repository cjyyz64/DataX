package com.alibaba.datax.plugin.reader.oceanbasereader.visitor;

import com.alibaba.datax.plugin.reader.oceanbasereader.Index;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.BinaryExpression;
import com.alibaba.datax.plugin.reader.oceanbasereader.ast.Operator;

import java.util.*;

public class IndexScanVisitor extends Visitor<Index.Entry> {

	private Set<Index> indexes = Collections.emptySet();
	
	private Map<Index.Entry,Integer> indexMap = new HashMap<Index.Entry, Integer>();
	
	private Set<String> prefix = new HashSet<String>();
	
	private static Map<Operator,Integer> WEIGHT = new HashMap<Operator, Integer>(4);

	static {
		WEIGHT.put(Operator.LE, -1);
		WEIGHT.put(Operator.LT, -1);
		WEIGHT.put(Operator.GT, 1);
		WEIGHT.put(Operator.GE, 1);
	}
	
	public IndexScanVisitor(Set<Index> indexes) {
		this.indexes = Collections.unmodifiableSet(indexes);
	}

	@Override
	public void visitBinaryExpression(BinaryExpression expr) {
		expr.left.accept(this);
		expr.right.accept(this);
		if(!(WEIGHT.containsKey(expr.operator) || expr.operator == Operator.EQ)) return;
		for (Index index : indexes) {
			for (Index.Entry entry : index) {
				if (entry.name.equalsIgnoreCase(expr.left.toString())) {
					if(expr.operator == Operator.EQ) {
						prefix.add(entry.name);
					}else{
						if(!WEIGHT.containsKey(expr.operator)) return;
						Integer weight = WEIGHT.get(expr.operator);
						if (indexMap.containsKey(entry)) {
							indexMap.put(entry, weight + indexMap.get(entry));
						} else {
							indexMap.put(entry, weight);
						}
					}
					return;
				}
			}
		}
	}

	@Override
	public Index.Entry visitResult() {
		for(Map.Entry<Index.Entry, Integer> pair : indexMap.entrySet()){
			if(pair.getValue() == 0 && this.meetPrefixEntryEQCase(pair.getKey().name))
				return pair.getKey();
		}
		return null;
	}

	private boolean meetPrefixEntryEQCase(String field){
		Set<Index> indexSet = new HashSet<Index>();
		for (Index index : indexes) {
			boolean belong = false;
			for (Index.Entry entry : index) {
				if (entry.name.equals(field)) {
					belong = true;
					break;
				}
			}
			if (belong)
				indexSet.add(index);
		}
		for (Index index : indexSet) {
			boolean eq = true;
			for (Index.Entry entry : index) {
				if (entry.name.equals(field)) {
					break;
				} else {
					if(!prefix.contains(entry.name)){
						eq = false;
						break;
					}
				}
			}
			if(eq) return true;
		}
		return false;
	}

}