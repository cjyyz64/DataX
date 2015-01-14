package com.alibaba.datax.plugin.reader.oceanbasereader.ast;

import com.google.common.base.Joiner;

import java.util.Collections;
import java.util.List;

public class SelectExpression {
	
	  public final List<Expression> columns;
	  public final String table;
	  public final Expression where;
	  
	  public SelectExpression(
	      List<Expression> projections, String table, Expression where) {
	    this.columns = Collections.unmodifiableList(projections);
	    this.table = table;
	    this.where = where;
	  }

	  public String toSQL(){
		  String hint = String.format("/*+read_cluster(slave), read_consistency(weak), index(%s primary)*/", table);
		  String sql = (where == null ? "select %s from %s" : "select %s from %s where %s");
		  return String.format(sql, hint + " " + Helper.joiner.join(columns), table, where);
	  }

	  private static class Helper{
		  static final Joiner joiner= Joiner.on(",");
	  }
}