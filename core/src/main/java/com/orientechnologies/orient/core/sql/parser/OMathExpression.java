/* Generated By:JJTree: Do not edit this line. OMathExpression.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.AggregationContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.metadata.OPath;
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OMathExpression extends SimpleNode {

  private static final Object NULL_VALUE = new Object();

  public OExpression getExpandContent() {
    throw new OCommandExecutionException("Invalid expand expression");
  }

  public boolean isDefinedFor(OResult currentRecord) {
    return true;
  }

  public boolean isDefinedFor(OElement currentRecord) {
    return true;
  }

  public boolean isIndexChain(OCommandContext ctx, OClass clazz) {
    return false;
  }

  public boolean isParentesis() {
    return false;
  }

  public enum Operator {
    STAR(10) {
      @Override
      public Number apply(Integer left, Integer right) {
        long result = left.longValue() * right.longValue();

        if (result < Integer.MAX_VALUE && result > Integer.MIN_VALUE) {
          return (int) result;
        }
        return result;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left * right;
      }

      @Override
      public Number apply(Float left, Float right) {
        double result = left.doubleValue() * right.doubleValue();

        if (result < Float.MAX_VALUE && result > Float.MIN_VALUE) {
          return (float) result;
        }
        return left * right;
      }

      @Override
      public Number apply(Double left, Double right) {
        return left * right;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return left.multiply(right);
      }

      @Override
      public Object apply(Object left, Object right) {
        if (left == null || right == null) {
          return null;
        }
        return super.apply(left, right);
      }
    },
    SLASH(10) {
      @Override
      public Number apply(Integer left, Integer right) {
        if (left % right == 0) {
          return left / right;
        }
        return ((double) left) / right;
      }

      @Override
      public Number apply(Long left, Long right) {
        if (left % right == 0) {
          return left / right;
        }
        return ((double) left) / right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return left / right;
      }

      @Override
      public Number apply(Double left, Double right) {
        return left / right;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return left.divide(right, BigDecimal.ROUND_HALF_UP);
      }

      @Override
      public Object apply(Object left, Object right) {
        if (left == null || right == null) {
          return null;
        }
        return super.apply(left, right);
      }
    },
    REM(10) {
      @Override
      public Number apply(Integer left, Integer right) {
        return left % right;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left % right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return left % right;
      }

      @Override
      public Number apply(Double left, Double right) {
        return left % right;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return left.remainder(right);
      }

      @Override
      public Object apply(Object left, Object right) {
        if (left == null || right == null) {
          return null;
        }
        return super.apply(left, right);
      }
    },
    PLUS(20) {
      @Override
      public Number apply(Integer left, Integer right) {
        final Integer sum = left + right;
        if (sum < 0 && left.intValue() > 0 && right.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return left.longValue() + right;
        return sum;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left + right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return left + right;
      }

      @Override
      public Number apply(Double left, Double right) {
        return left + right;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return left.add(right);
      }

      @Override
      public Object apply(Object left, Object right) {
        if (left == null && right == null) {
          return null;
        }
        if (left == null) {
          return right;
        }
        if (right == null) {
          return left;
        }
        if (left instanceof Number && right instanceof Number) {
          return super.apply(left, right);
        }
        if (left instanceof Date || right instanceof Date) {
          Number result = apply(toLong(left), toLong(right));
          return new Date(result.longValue());
        }
        return String.valueOf(left) + String.valueOf(right);
      }
    },
    MINUS(20) {
      @Override
      public Number apply(Integer left, Integer right) {
        int result = left - right;
        if (result > 0 && left.intValue() < 0 && right.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return left.longValue() - right;

        return result;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left - right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return left - right;
      }

      @Override
      public Number apply(Double left, Double right) {
        return left - right;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return left.subtract(right);
      }

      @Override
      public Object apply(Object left, Object right) {
        Object result = null;
        if (left == null && right == null) {
          result = null;
        } else if (left instanceof Number && right == null) {
          result = left;
        } else if (right instanceof Number && left == null) {
          result = apply(0, this, (Number) right);
        } else if (left instanceof Number && right instanceof Number) {
          result = apply((Number) left, this, (Number) right);
        } else if (left instanceof Date || right instanceof Date) {
          Number r = apply(toLong(left), toLong(right));
          result = new Date(r.longValue());
        }

        return result;
      }
    },
    LSHIFT(30) {
      @Override
      public Number apply(Integer left, Integer right) {
        return left << right;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left << right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return null;
      }

      @Override
      public Number apply(Double left, Double right) {
        return null;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return null;
      }

      @Override
      public Object apply(Object left, Object right) {
        if (left == null || right == null) {
          return null;
        }
        return super.apply(left, right);
      }
    },
    RSHIFT(30) {
      @Override
      public Number apply(Integer left, Integer right) {
        return left >> right;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left >> right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return null;
      }

      @Override
      public Number apply(Double left, Double right) {
        return null;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return null;
      }

      @Override
      public Object apply(Object left, Object right) {
        if (left == null || right == null) {
          return null;
        }
        return super.apply(left, right);
      }
    },
    RUNSIGNEDSHIFT(30) {
      @Override
      public Number apply(Integer left, Integer right) {
        return left >>> right;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left >>> right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return null;
      }

      @Override
      public Number apply(Double left, Double right) {
        return null;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return null;
      }

      @Override
      public Object apply(Object left, Object right) {
        if (left == null || right == null) {
          return null;
        }
        return super.apply(left, right);
      }
    },
    BIT_AND(40) {
      @Override
      public Number apply(Integer left, Integer right) {
        return left & right;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left & right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return apply(left.longValue(), right.longValue());
      }

      @Override
      public Number apply(Double left, Double right) {
        return apply(left.longValue(), right.longValue());
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return null;
      }

      public Object apply(Object left, Object right) {
        if (left == null || right == null) {
          return null;
        }
        return super.apply(left, right);
      }
    },
    XOR(50) {
      @Override
      public Number apply(Integer left, Integer right) {
        return left ^ right;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left ^ right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return null;
      }

      @Override
      public Number apply(Double left, Double right) {
        return null;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return null;
      }

      @Override
      public Object apply(Object left, Object right) {
        if (left == null && right == null) {
          return null;
        }
        if (left instanceof Number && right == null) {
          return apply((Number) left, this, 0);
        }
        if (right instanceof Number && left == null) {
          return apply(0, this, (Number) right);
        }

        if (left instanceof Number && right instanceof Number) {
          return apply((Number) left, this, (Number) right);
        }

        return null;
      }
    },
    BIT_OR(60) {
      @Override
      public Number apply(Integer left, Integer right) {
        return left | right;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left | right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return null;
      }

      @Override
      public Number apply(Double left, Double right) {
        return null;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return null;
      }

      public Object apply(Object left, Object right) {
        if (left == null && right == null) {
          return null;
        }
        return super.apply(left == null ? 0 : left, right == null ? 0 : right);
      }
    },
    NULL_COALESCING(25) {
      @Override
      public Number apply(Integer left, Integer right) {
        return left != null ? left : right;
      }

      @Override
      public Number apply(Long left, Long right) {
        return left != null ? left : right;
      }

      @Override
      public Number apply(Float left, Float right) {
        return left != null ? left : right;
      }

      @Override
      public Number apply(Double left, Double right) {
        return left != null ? left : right;
      }

      @Override
      public Number apply(BigDecimal left, BigDecimal right) {
        return left != null ? left : right;
      }

      public Object apply(Object left, Object right) {
        return left != null ? left : right;
      }
    };

    private static Long toLong(Object left) {
      if (left instanceof Number) {
        return ((Number) left).longValue();
      }
      if (left instanceof Date) {
        return ((Date) left).getTime();
      }
      return null;
    }

    private final int priority;

    Operator(int priority) {
      this.priority = priority;
    }

    public abstract Number apply(Integer left, Integer right);

    public abstract Number apply(Long left, Long right);

    public abstract Number apply(Float left, Float right);

    public abstract Number apply(Double left, Double right);

    public abstract Number apply(BigDecimal left, BigDecimal right);

    public Object apply(Object left, Object right) {
      if (left == null) {
        return right;
      }
      if (right == null) {
        return left;
      }
      if (left instanceof Number && right instanceof Number) {
        return apply((Number) left, this, (Number) right);
      }

      return null;
    }

    public Number apply(final Number a, final Operator operation, final Number b) {
      if (a == null || b == null)
        throw new IllegalArgumentException("Cannot increment a null value");

      if (a instanceof Integer || a instanceof Short) {
        if (b instanceof Integer || b instanceof Short) {
          return operation.apply(a.intValue(), b.intValue());
        } else if (b instanceof Long) {
          return operation.apply(a.longValue(), b.longValue());
        } else if (b instanceof Float) return operation.apply(a.floatValue(), b.floatValue());
        else if (b instanceof Double) return operation.apply(a.doubleValue(), b.doubleValue());
        else if (b instanceof BigDecimal)
          return operation.apply(new BigDecimal((Integer) a), (BigDecimal) b);
      } else if (a instanceof Long) {
        if (b instanceof Integer || b instanceof Long || b instanceof Short)
          return operation.apply(a.longValue(), b.longValue());
        else if (b instanceof Float) return operation.apply(a.floatValue(), b.floatValue());
        else if (b instanceof Double) return operation.apply(a.doubleValue(), b.doubleValue());
        else if (b instanceof BigDecimal)
          return operation.apply(new BigDecimal((Long) a), (BigDecimal) b);
      } else if (a instanceof Float) {
        if (b instanceof Short || b instanceof Integer || b instanceof Long || b instanceof Float)
          return operation.apply(a.floatValue(), b.floatValue());
        else if (b instanceof Double) return operation.apply(a.doubleValue(), b.doubleValue());
        else if (b instanceof BigDecimal)
          return operation.apply(new BigDecimal((Float) a), (BigDecimal) b);

      } else if (a instanceof Double) {
        if (b instanceof Short
            || b instanceof Integer
            || b instanceof Long
            || b instanceof Float
            || b instanceof Double) return operation.apply(a.doubleValue(), b.doubleValue());
        else if (b instanceof BigDecimal)
          return operation.apply(new BigDecimal((Double) a), (BigDecimal) b);

      } else if (a instanceof BigDecimal) {
        if (b instanceof Integer)
          return operation.apply((BigDecimal) a, new BigDecimal((Integer) b));
        else if (b instanceof Long)
          return operation.apply((BigDecimal) a, new BigDecimal((Long) b));
        else if (b instanceof Short)
          return operation.apply((BigDecimal) a, new BigDecimal((Short) b));
        else if (b instanceof Float)
          return operation.apply((BigDecimal) a, new BigDecimal((Float) b));
        else if (b instanceof Double)
          return operation.apply((BigDecimal) a, new BigDecimal((Double) b));
        else if (b instanceof BigDecimal) return operation.apply((BigDecimal) a, (BigDecimal) b);
      }

      throw new IllegalArgumentException(
          "Cannot increment value '"
              + a
              + "' ("
              + a.getClass()
              + ") with '"
              + b
              + "' ("
              + b.getClass()
              + ")");
    }

    public int getPriority() {
      return priority;
    }
  }

  protected List<OMathExpression> childExpressions;
  protected List<Operator> operators;

  public OMathExpression(int id) {
    super(id);
  }

  public OMathExpression(OrientSql p, int id) {
    super(p, id);
  }

  public boolean isCacheable() {
    if (childExpressions != null) {
      for (OMathExpression exp : childExpressions) {
        if (!exp.isCacheable()) {
          return false;
        }
      }
    }
    return true;
  }

  public Object execute(OResult iCurrentRecord, OCommandContext ctx) {
    if (childExpressions == null || operators == null) {
      return null;
    }
    if (childExpressions.size() == 0) {
      return null;
    }
    if (childExpressions.size() == 1) {
      return childExpressions.get(0).execute(iCurrentRecord, ctx);
    }

    if (childExpressions.size() == 2) {
      Object leftValue = childExpressions.get(0).execute(iCurrentRecord, ctx);
      Object rightValue = childExpressions.get(1).execute(iCurrentRecord, ctx);
      return operators.get(0).apply(leftValue, rightValue);
    }

    return calculateWithOpPriority(iCurrentRecord, ctx);
  }

  private Object calculateWithOpPriority(OResult iCurrentRecord, OCommandContext ctx) {
    Deque valuesStack = new ArrayDeque<>();
    Deque<Operator> operatorsStack = new ArrayDeque<Operator>();
    if (childExpressions != null && operators != null) {
      OMathExpression nextExpression = childExpressions.get(0);
      Object val = nextExpression.execute(iCurrentRecord, ctx);
      valuesStack.push(val == null ? NULL_VALUE : val);

      for (int i = 0; i < operators.size() && i + 1 < childExpressions.size(); i++) {
        Operator nextOperator = operators.get(i);
        Object rightValue = childExpressions.get(i + 1).execute(iCurrentRecord, ctx);

        if (!operatorsStack.isEmpty()
            && operatorsStack.peek().getPriority() <= nextOperator.getPriority()) {
          Object right = valuesStack.poll();
          right = right == NULL_VALUE ? null : right;
          Object left = valuesStack.poll();
          left = left == NULL_VALUE ? null : left;
          Object calculatedValue = operatorsStack.poll().apply(left, right);
          valuesStack.push(calculatedValue == null ? NULL_VALUE : calculatedValue);
        }
        operatorsStack.push(nextOperator);

        valuesStack.push(rightValue == null ? NULL_VALUE : rightValue);
      }
    }

    return iterateOnPriorities(valuesStack, operatorsStack);
  }

  private Object iterateOnPriorities(Deque values, Deque<Operator> operators) {
    while (true) {
      if (values.size() == 0) {
        return null;
      }
      if (values.size() == 1) {
        return values.getFirst();
      }

      Deque valuesStack = new ArrayDeque<>();
      Deque<OMathExpression.Operator> operatorsStack = new ArrayDeque<OMathExpression.Operator>();

      valuesStack.push(values.removeLast());

      while (!operators.isEmpty()) {
        Operator nextOperator = operators.removeLast();
        Object rightValue = values.removeLast();

        if (!operatorsStack.isEmpty()
            && operatorsStack.peek().getPriority() <= nextOperator.getPriority()) {
          Object right = valuesStack.poll();
          right = right == NULL_VALUE ? null : right;
          Object left = valuesStack.poll();
          left = left == NULL_VALUE ? null : left;
          Object calculatedValue = operatorsStack.poll().apply(left, right);
          valuesStack.push(calculatedValue == null ? NULL_VALUE : calculatedValue);
        }
        operatorsStack.push(nextOperator);
        valuesStack.push(rightValue == null ? NULL_VALUE : rightValue);
      }
      if (!operatorsStack.isEmpty()) {
        Object right = valuesStack.poll();
        right = right == NULL_VALUE ? null : right;
        Object left = valuesStack.poll();
        left = left == NULL_VALUE ? null : left;
        Object val = operatorsStack.poll().apply(left, right);
        valuesStack.push(val == null ? NULL_VALUE : val);
      }

      values = valuesStack;
      operators = operatorsStack;
    }
  }

  public List<OMathExpression> getChildExpressions() {
    return childExpressions;
  }

  public void setChildExpressions(List<OMathExpression> childExpressions) {
    this.childExpressions = childExpressions;
  }

  public void addChildExpression(OMathExpression expression) {
    if (this.childExpressions == null) {
      this.childExpressions = new ArrayList<>();
    }
    this.childExpressions.add(expression);
  }

  public OMathExpression unwrapIfNeeded() {
    if (this.childExpressions != null && this.childExpressions.size() == 1) {
      return this.childExpressions.get(0);
    }
    return this;
  }

  public List<Operator> getOperators() {
    return operators;
  }

  public void addOperator(Operator operator) {
    if (this.operators == null) {
      this.operators = new ArrayList<>();
    }
    this.operators.add(operator);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (childExpressions == null || operators == null) return;
    for (int i = 0; i < childExpressions.size(); i++) {
      if (i > 0) {
        builder.append(" ");
        switch (operators.get(i - 1)) {
          case PLUS:
            builder.append("+");
            break;
          case MINUS:
            builder.append("-");
            break;
          case STAR:
            builder.append("*");
            break;
          case SLASH:
            builder.append("/");
            break;
          case REM:
            builder.append("%");
            break;
          case LSHIFT:
            builder.append("<<");
            break;
          case RSHIFT:
            builder.append(">>");
            break;
          case RUNSIGNEDSHIFT:
            builder.append(">>>");
            break;
          case BIT_AND:
            builder.append("&");
            break;
          case BIT_OR:
            builder.append("|");
            break;
          case XOR:
            builder.append("^");
            break;
        }
        builder.append(" ");
      }
      childExpressions.get(i).toString(params, builder);
    }
  }

  public void toGenericStatement(StringBuilder builder) {
    if (childExpressions == null || operators == null) return;
    for (int i = 0; i < childExpressions.size(); i++) {
      if (i > 0) {
        builder.append(" ");
        switch (operators.get(i - 1)) {
          case PLUS:
            builder.append("+");
            break;
          case MINUS:
            builder.append("-");
            break;
          case STAR:
            builder.append("*");
            break;
          case SLASH:
            builder.append("/");
            break;
          case REM:
            builder.append("%");
            break;
          case LSHIFT:
            builder.append("<<");
            break;
          case RSHIFT:
            builder.append(">>");
            break;
          case RUNSIGNEDSHIFT:
            builder.append(">>>");
            break;
          case BIT_AND:
            builder.append("&");
            break;
          case BIT_OR:
            builder.append("|");
            break;
          case XOR:
            builder.append("^");
            break;
        }
        builder.append(" ");
      }
      childExpressions.get(i).toGenericStatement(builder);
    }
  }

  protected boolean supportsBasicCalculation() {
    if (this.childExpressions != null) {
      for (OMathExpression expr : this.childExpressions) {
        if (!expr.supportsBasicCalculation()) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean isIndexedFunctionCall() {
    if (this.childExpressions != null) {
      if (this.childExpressions.size() == 1) {
        return this.childExpressions.get(0).isIndexedFunctionCall();
      }
    }
    return false;
  }

  public long estimateIndexedFunction(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (this.childExpressions != null) {
      if (this.childExpressions.size() == 1) {
        return this.childExpressions
            .get(0)
            .estimateIndexedFunction(target, context, operator, right);
      }
    }
    return -1;
  }

  public Iterable<OIdentifiable> executeIndexedFunction(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (this.childExpressions != null) {
      if (this.childExpressions.size() == 1) {
        return this.childExpressions
            .get(0)
            .executeIndexedFunction(target, context, operator, right);
      }
    }
    return null;
  }

  /**
   * tests if current expression is an indexed function AND that function can also be executed
   * without using the index
   *
   * @param target the query target
   * @param context the execution context
   * @return true if current expression is an indexed function AND that function can also be
   *     executed without using the index, false otherwise
   */
  public boolean canExecuteIndexedFunctionWithoutIndex(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (this.childExpressions != null) {
      if (this.childExpressions.size() == 1) {
        return this.childExpressions
            .get(0)
            .canExecuteIndexedFunctionWithoutIndex(target, context, operator, right);
      }
    }
    return false;
  }

  /**
   * tests if current expression is an indexed function AND that function can be used on this target
   *
   * @param target the query target
   * @param context the execution context
   * @return true if current expression is an indexed function AND that function can be used on this
   *     target, false otherwise
   */
  public boolean allowsIndexedFunctionExecutionOnTarget(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (this.childExpressions != null) {
      if (this.childExpressions.size() == 1) {
        return this.childExpressions
            .get(0)
            .allowsIndexedFunctionExecutionOnTarget(target, context, operator, right);
      }
    }

    return false;
  }

  /**
   * tests if current expression is an indexed function AND the function has also to be executed
   * after the index search. In some cases, the index search is accurate, so this condition can be
   * excluded from further evaluation. In other cases the result from the index is a superset of the
   * expected result, so the function has to be executed anyway for further filtering
   *
   * @param target the query target
   * @param context the execution context
   * @return true if current expression is an indexed function AND the function has also to be
   *     executed after the index search.
   */
  public boolean executeIndexedFunctionAfterIndexSearch(
      OFromClause target, OCommandContext context, OBinaryCompareOperator operator, Object right) {
    if (this.childExpressions != null) {
      if (this.childExpressions.size() == 1) {
        return this.childExpressions
            .get(0)
            .executeIndexedFunctionAfterIndexSearch(target, context, operator, right);
      }
    }
    return false;
  }

  public boolean isFunctionAny() {
    if (this.childExpressions != null) {
      if (this.childExpressions.size() == 1) {
        return this.childExpressions.get(0).isFunctionAny();
      }
    }
    return false;
  }

  public boolean isFunctionAll() {
    if (this.childExpressions != null) {
      if (this.childExpressions.size() == 1) {
        return this.childExpressions.get(0).isFunctionAll();
      }
    }
    return false;
  }

  public boolean isBaseIdentifier() {
    if (this.childExpressions != null) {
      if (childExpressions.size() == 1) {
        return childExpressions.get(0).isBaseIdentifier();
      }
    }
    return false;
  }

  public Optional<OPath> getPath() {
    if (this.childExpressions != null) {
      if (childExpressions.size() == 1) {
        return childExpressions.get(0).getPath();
      }
    }
    return Optional.empty();
  }

  public OCollate getCollate(OResult currentRecord, OCommandContext ctx) {
    if (this.childExpressions != null) {
      if (childExpressions.size() == 1)
        return childExpressions.get(0).getCollate(currentRecord, ctx);
    }
    return null;
  }

  public boolean isEarlyCalculated(OCommandContext ctx) {
    if (this.childExpressions != null) {
      for (OMathExpression exp : childExpressions) {
        if (!exp.isEarlyCalculated(ctx)) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean needsAliases(Set<String> aliases) {
    if (this.childExpressions != null) {
      for (OMathExpression expr : childExpressions) {
        if (expr.needsAliases(aliases)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean isExpand() {
    if (this.childExpressions != null) {

      for (OMathExpression expr : this.childExpressions) {
        if (expr.isExpand()) {
          if (this.childExpressions.size() > 1) {
            throw new OCommandExecutionException(
                "Cannot calculate expand() with other expressions");
          }
          return true;
        }
      }
    }
    return false;
  }

  public boolean isAggregate() {
    if (this.childExpressions != null) {
      for (OMathExpression expr : this.childExpressions) {
        if (expr.isAggregate()) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean isCount() {
    if (this.childExpressions == null) {
      return false;
    }
    if (this.childExpressions.size() != 1) {
      return false;
    }
    return this.childExpressions.get(0).isCount();
  }

  public SimpleNode splitForAggregation(
      AggregateProjectionSplit aggregateProj, OCommandContext ctx) {
    if (isAggregate()) {
      OMathExpression result = new OMathExpression(-1);
      if (this.childExpressions != null && this.operators != null) {
        int i = 0;
        for (OMathExpression expr : this.childExpressions) {
          if (i > 0) {
            result.addOperator(operators.get(i - 1));
          }
          SimpleNode splitResult = expr.splitForAggregation(aggregateProj, ctx);
          if (splitResult instanceof OMathExpression) {
            OMathExpression res = (OMathExpression) splitResult;
            if (res.isEarlyCalculated(ctx) || res.isAggregate()) {
              result.addChildExpression(res);
            } else {
              throw new OCommandExecutionException(
                  "Cannot mix aggregate and single record attribute values in the same projection");
            }
          } else if (splitResult instanceof OExpression) {
            result.addChildExpression(
                ((OExpression) splitResult)
                    .mathExpression); // this comes from a splitted aggregate function
          }
          i++;
        }
      }
      return result;
    } else {
      return this;
    }
  }

  public AggregationContext getAggregationContext(OCommandContext ctx) {
    throw new UnsupportedOperationException(
        "multiple math expressions do not allow plain aggregation");
  }

  public OMathExpression copy() {
    OMathExpression result = null;
    try {
      result = getClass().getConstructor(Integer.TYPE).newInstance(-1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (this.childExpressions != null) {
      result.childExpressions =
          childExpressions.stream().map(x -> x.copy()).collect(Collectors.toList());
    }
    if (operators != null) {
      result.operators = new ArrayList<>(operators);
    }
    return result;
  }

  public void extractSubQueries(OIdentifier letAlias, SubQueryCollector collector) {
    if (this.childExpressions != null) {
      for (OMathExpression expr : this.childExpressions) {
        expr.extractSubQueries(letAlias, collector);
      }
    }
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (this.childExpressions != null) {
      for (OMathExpression expr : this.childExpressions) {
        expr.extractSubQueries(collector);
      }
    }
  }

  public boolean refersToParent() {
    if (this.childExpressions != null) {
      for (OMathExpression expr : this.childExpressions) {
        if (expr.refersToParent()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OMathExpression that = (OMathExpression) o;

    if (childExpressions != null
        ? !childExpressions.equals(that.childExpressions)
        : that.childExpressions != null) return false;
    if (operators != null ? !operators.equals(that.operators) : that.operators != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = childExpressions != null ? childExpressions.hashCode() : 0;
    result = 31 * result + (operators != null ? operators.hashCode() : 0);
    return result;
  }

  public List<String> getMatchPatternInvolvedAliases() {
    List<String> result = new ArrayList<String>();
    if (this.childExpressions != null) {
      for (OMathExpression exp : childExpressions) {
        List<String> x = exp.getMatchPatternInvolvedAliases();
        if (x != null) {
          result.addAll(x);
        }
      }
    }
    if (result.size() == 0) {
      return null;
    }
    return result;
  }

  public void applyRemove(OResultInternal result, OCommandContext ctx) {
    if (childExpressions == null || childExpressions.size() != 1) {
      throw new OCommandExecutionException("cannot apply REMOVE " + toString());
    }
    childExpressions.get(0).applyRemove(result, ctx);
  }

  public static OMathExpression deserializeFromResult(OResult fromResult) {
    String className = fromResult.getProperty("__class");
    try {
      OMathExpression result =
          (OMathExpression) Class.forName(className).getConstructor(Integer.class).newInstance(-1);
      result.deserialize(fromResult);
      return result;
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("__class", getClass().getName());
    if (childExpressions != null) {
      result.setProperty(
          "childExpressions",
          childExpressions.stream().map(x -> x.serialize()).collect(Collectors.toList()));
    }
    if (operators != null) {
      result.setProperty(
          "operators",
          operators.stream().map(x -> serializeOperator(x)).collect(Collectors.toList()));
    }
    return result;
  }

  public void deserialize(OResult fromResult) {
    if (fromResult.getProperty("childExpressions") != null) {
      List<OResult> ser = fromResult.getProperty("childExpressions");
      childExpressions =
          ser.stream().map(x -> deserializeFromResult(x)).collect(Collectors.toList());
    }
    if (fromResult.getProperty("operators") != null) {
      List<String> ser = fromResult.getProperty("operators");
      operators = ser.stream().map(x -> deserializeOperator(x)).collect(Collectors.toList());
    }
  }

  private String serializeOperator(Operator x) {
    return x.toString();
  }

  private Operator deserializeOperator(String x) {
    return Operator.valueOf(x);
  }

  public boolean isSelf() {
    return false;
  }
}
/* JavaCC - OriginalChecksum=c255bea24e12493e1005ba2a4d1dbb9d (do not edit this line) */
