/* Generated By:JJTree: Do not edit this line. OContainsAllCondition.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OContainsAllCondition extends OBooleanExpression {

  protected OExpression left;

  protected OExpression right;

  protected OOrBlock rightBlock;

  public OContainsAllCondition(int id) {
    super(id);
  }

  public OContainsAllCondition(OrientSql p, int id) {
    super(p, id);
  }

  public boolean execute(Object left, Object right) {
    if (left instanceof Collection) {
      if (right instanceof Collection) {
        if (((Collection) left).containsAll((Collection) right)) {
          return true;
        }
      }
      if (right instanceof Iterable) {
        right = ((Iterable) right).iterator();
      }
      if (right instanceof Iterator) {
        Iterator iterator = (Iterator) right;
        while (iterator.hasNext()) {
          Object next = iterator.next();
          boolean found = false;
          if (((Collection) left).contains(next)) {
            found = true;
          } else if (next instanceof OResult
              && ((OResult) next).isElement()
              && ((Collection) left).contains(((OResult) next).getElement().get())) {
            found = true;
          }
          if (!found) {
            return false;
          }
        }
        return true;
      }
      return ((Collection) left).contains(right);
    }
    if (left instanceof Iterable) {
      left = ((Iterable) left).iterator();
    }
    if (left instanceof Iterator) {
      if (!(right instanceof Iterable)) {
        right = Collections.singleton(right);
      }
      right = ((Iterable) right).iterator();

      Iterator leftIterator = (Iterator) left;
      Iterator rightIterator = (Iterator) right;
      while (rightIterator.hasNext()) {
        Object leftItem = rightIterator.next();
        boolean found = false;
        while (leftIterator.hasNext()) {
          Object rightItem = leftIterator.next();
          if (leftItem != null && leftItem.equals(rightItem)) {
            found = true;
            break;
          }
        }
        if (!found) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean evaluate(OResult currentRecord, OCommandContext ctx) {
    if (left.isFunctionAny()) {
      return evaluateAny(currentRecord, ctx);
    }

    if (left.isFunctionAll()) {
      return evaluateAllFunction(currentRecord, ctx);
    }

    Object leftValue = left.execute(currentRecord, ctx);
    return evaluateSingle(leftValue, currentRecord, ctx);
  }

  private boolean evaluateAllFunction(OResult currentRecord, OCommandContext ctx) {
    for (String propertyName : currentRecord.getPropertyNames()) {
      Object leftValue = currentRecord.getProperty(propertyName);
      if (!evaluateSingle(leftValue, currentRecord, ctx)) {
        return false;
      }
    }
    return true;
  }

  private boolean evaluateAny(OResult currentRecord, OCommandContext ctx) {
    for (String propertyName : currentRecord.getPropertyNames()) {
      Object leftValue = currentRecord.getProperty(propertyName);
      if (evaluateSingle(leftValue, currentRecord, ctx)) {
        return true;
      }
    }
    return false;
  }

  private boolean evaluateSingle(Object leftValue, OResult currentRecord, OCommandContext ctx) {
    if (right != null) {
      Object rightValue = right.execute(currentRecord, ctx);
      return execute(leftValue, rightValue);
    } else {
      if (!OMultiValue.isMultiValue(leftValue)) {
        return false;
      }
      Iterator<Object> iter = OMultiValue.getMultiValueIterator(leftValue);
      while (iter.hasNext()) {
        Object item = iter.next();
        if (item instanceof OIdentifiable) {
          if (!rightBlock.evaluate(new OResultInternal((OIdentifiable) item), ctx)) {
            return false;
          }
        } else if (item instanceof OResult) {
          if (!rightBlock.evaluate((OResult) item, ctx)) {
            return false;
          }
        } else {
          return false;
        }
      }
      return true;
    }
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    left.toString(params, builder);
    builder.append(" CONTAINSALL ");
    if (right != null) {
      right.toString(params, builder);
    } else if (rightBlock != null) {
      builder.append("(");
      rightBlock.toString(params, builder);
      builder.append(")");
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    left.toGenericStatement(builder);
    builder.append(" CONTAINSALL ");
    if (right != null) {
      right.toGenericStatement(builder);
    } else if (rightBlock != null) {
      builder.append("(");
      rightBlock.toGenericStatement(builder);
      builder.append(")");
    }
  }

  public OExpression getLeft() {
    return left;
  }

  public void setLeft(OExpression left) {
    this.left = left;
  }

  public OExpression getRight() {
    return right;
  }

  public void setRight(OExpression right) {
    this.right = right;
  }

  @Override
  public boolean supportsBasicCalculation() {
    if (left != null && !left.supportsBasicCalculation()) {
      return false;
    }
    if (right != null && !right.supportsBasicCalculation()) {
      return false;
    }
    if (rightBlock != null && !rightBlock.supportsBasicCalculation()) {
      return false;
    }
    return true;
  }

  @Override
  protected int getNumberOfExternalCalculations() {
    int total = 0;
    if (left != null && !left.supportsBasicCalculation()) {
      total++;
    }
    if (right != null && !right.supportsBasicCalculation()) {
      total++;
    }
    if (rightBlock != null && !rightBlock.supportsBasicCalculation()) {
      total++;
    }
    return total;
  }

  @Override
  protected List<Object> getExternalCalculationConditions() {
    List<Object> result = new ArrayList<Object>();
    if (left != null && !left.supportsBasicCalculation()) {
      result.add(left);
    }
    if (right != null && !right.supportsBasicCalculation()) {
      result.add(right);
    }
    if (rightBlock != null) {
      result.addAll(rightBlock.getExternalCalculationConditions());
    }
    return result;
  }

  @Override
  public boolean needsAliases(Set<String> aliases) {
    if (left.needsAliases(aliases)) {
      return true;
    }

    if (right != null && right.needsAliases(aliases)) {
      return true;
    }
    if (rightBlock != null && rightBlock.needsAliases(aliases)) {
      return true;
    }
    return false;
  }

  @Override
  public OContainsAllCondition copy() {
    OContainsAllCondition result = new OContainsAllCondition(-1);
    result.left = left.copy();
    result.right = right == null ? null : right.copy();
    result.rightBlock = rightBlock == null ? null : rightBlock.copy();
    return result;
  }

  @Override
  public void extractSubQueries(SubQueryCollector collector) {
    left.extractSubQueries(collector);
    if (right != null) {
      right.extractSubQueries(collector);
    }
    if (rightBlock != null) {
      rightBlock.extractSubQueries(collector);
    }
  }

  @Override
  public boolean refersToParent() {
    if (left != null && left.refersToParent()) {
      return true;
    }
    if (right != null && right.refersToParent()) {
      return true;
    }
    if (rightBlock != null && rightBlock.refersToParent()) {
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OContainsAllCondition that = (OContainsAllCondition) o;

    if (left != null ? !left.equals(that.left) : that.left != null) return false;
    if (right != null ? !right.equals(that.right) : that.right != null) return false;
    if (rightBlock != null ? !rightBlock.equals(that.rightBlock) : that.rightBlock != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = left != null ? left.hashCode() : 0;
    result = 31 * result + (right != null ? right.hashCode() : 0);
    result = 31 * result + (rightBlock != null ? rightBlock.hashCode() : 0);
    return result;
  }

  @Override
  public List<String> getMatchPatternInvolvedAliases() {
    List<String> leftX = left == null ? null : left.getMatchPatternInvolvedAliases();
    List<String> rightX = right == null ? null : right.getMatchPatternInvolvedAliases();
    List<String> rightBlockX =
        rightBlock == null ? null : rightBlock.getMatchPatternInvolvedAliases();

    List<String> result = new ArrayList<String>();
    if (leftX != null) {
      result.addAll(leftX);
    }
    if (rightX != null) {
      result.addAll(rightX);
    }
    if (rightBlockX != null) {
      result.addAll(rightBlockX);
    }

    return result.size() == 0 ? null : result;
  }

  @Override
  public boolean isCacheable() {
    if (left != null && !left.isCacheable()) {
      return false;
    }

    if (right != null && !right.isCacheable()) {
      return false;
    }

    if (rightBlock != null && !rightBlock.isCacheable()) {
      return false;
    }
    return true;
  }
}
/* JavaCC - OriginalChecksum=ab7b4e192a01cda09a82d5b80ef4ec60 (do not edit this line) */
