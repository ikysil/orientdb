/* Generated By:JJTree: Do not edit this line. OIsDefinedCondition.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OIsDefinedCondition extends OBooleanExpression {

  protected OExpression expression;

  public OIsDefinedCondition(int id) {
    super(id);
  }

  public OIsDefinedCondition(OrientSql p, int id) {
    super(p, id);
  }

  /**
   * Accept the visitor.
   **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override public boolean evaluate(OIdentifiable currentRecord, OCommandContext ctx) {
    throw new UnsupportedOperationException("TODO Implement IS DEFINED!!!");//TODO
  }

  @Override public boolean evaluate(OResult currentRecord, OCommandContext ctx) {
    throw new UnsupportedOperationException("TODO Implement IS DEFINED!!!");//TODO
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    expression.toString(params, builder);
    builder.append(" is defined");
  }

  @Override public boolean supportsBasicCalculation() {
    return true;
  }

  @Override protected int getNumberOfExternalCalculations() {
    return 0;
  }

  @Override protected List<Object> getExternalCalculationConditions() {
    return Collections.EMPTY_LIST;
  }

  @Override public boolean needsAliases(Set<String> aliases) {
    return expression.needsAliases(aliases);
  }

  @Override public OIsDefinedCondition copy() {
    OIsDefinedCondition result = new OIsDefinedCondition(-1);
    result.expression = expression.copy();
    return result;
  }

  @Override public void extractSubQueries(SubQueryCollector collector) {
    this.expression.extractSubQueries(collector);
  }

  @Override public boolean refersToParent() {
    if (expression != null && expression.refersToParent()) {
      return true;
    }
    return false;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OIsDefinedCondition that = (OIsDefinedCondition) o;

    if (expression != null ? !expression.equals(that.expression) : that.expression != null)
      return false;

    return true;
  }

  @Override public int hashCode() {
    return expression != null ? expression.hashCode() : 0;
  }
}
/* JavaCC - OriginalChecksum=075954b212c8cb44c8538bf5dea047d3 (do not edit this line) */
