/* Generated By:JJTree: Do not edit this line. OExpression.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.AggregationContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * this class is only used by the query executor to store pre-calculated values and store them in a
 * temporary AST. It's not produced by parsing
 */
public class OValueExpression extends OExpression {

  public OValueExpression(Object val) {
    super(-1);
    this.value = val;
  }

  public Object execute(OResult iCurrentRecord, OCommandContext ctx) {
    return value;
  }

  public boolean isBaseIdentifier() {
    return false;
  }

  public boolean isEarlyCalculated() {
    return true;
  }

  public OIdentifier getDefaultAlias() {
    return new OIdentifier(String.valueOf(value));
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(String.valueOf(value));
  }

  public boolean supportsBasicCalculation() {
    return true;
  }

  public boolean isIndexedFunctionCal() {
    return false;
  }

  public boolean canExecuteIndexedFunctionWithoutIndex(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    return false;
  }

  public boolean allowsIndexedFunctionExecutionOnTarget(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    return false;
  }

  public boolean executeIndexedFunctionAfterIndexSearch(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    return false;
  }

  public boolean isExpand() {
    return false;
  }

  public OValueExpression getExpandContent() {
    return null;
  }

  public boolean needsAliases(Set<String> aliases) {
    return false;
  }

  public boolean isAggregate() {
    return false;
  }

  public OValueExpression splitForAggregation(AggregateProjectionSplit aggregateSplit) {
    return this;
  }

  public AggregationContext getAggregationContext(OCommandContext ctx) {
    throw new OCommandExecutionException("Cannot aggregate on " + toString());
  }

  public OValueExpression copy() {

    OValueExpression result = new OValueExpression(-1);
    result.value = value;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OValueExpression that = (OValueExpression) o;
    return that.value == this.value;
  }

  @Override
  public int hashCode() {
    return 1;
  }

  public void extractSubQueries(SubQueryCollector collector) {}

  public void extractSubQueries(OIdentifier letAlias, SubQueryCollector collector) {}

  public boolean refersToParent() {

    return false;
  }

  List<String> getMatchPatternInvolvedAliases() {
    return null;
  }

  public void applyRemove(OResultInternal result, OCommandContext ctx) {
    throw new OCommandExecutionException("Cannot apply REMOVE " + toString());
  }

  public boolean isCount() {
    return false;
  }

  public OResult serialize() {
    throw new UnsupportedOperationException(
        "Cannot serialize value expression (not supported yet)");
  }

  public void deserialize(OResult fromResult) {
    throw new UnsupportedOperationException(
        "Cannot deserialize value expression (not supported yet)");
  }

  public boolean isDefinedFor(OResult currentRecord) {
    return true;
  }

  public boolean isDefinedFor(OElement currentRecord) {
    return true;
  }

  public OCollate getCollate(OResult currentRecord, OCommandContext ctx) {
    return null;
  }

  public boolean isCacheable() {
    return true;
  }
}
/* JavaCC - OriginalChecksum=9c860224b121acdc89522ae97010be01 (do not edit this line) */
