package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.ODeleteVertexStatement;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OLimit;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

/** Created by luigidellaquila on 08/08/16. */
public class ODeleteVertexExecutionPlanner {

  private final OFromClause fromClause;
  private final OWhereClause whereClause;
  private final boolean returnBefore;
  private final OLimit limit;

  public ODeleteVertexExecutionPlanner(ODeleteVertexStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit() == null ? null : stm.getLimit();
  }

  public ODeleteExecutionPlan createExecutionPlan(OCommandContext ctx) {
    ODeleteExecutionPlan result = new ODeleteExecutionPlan();

    if (handleIndexAsTarget(result, fromClause.getItem().getIndex(), whereClause, ctx)) {
      if (limit != null) {
        throw new OCommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (returnBefore) {
        throw new OCommandExecutionException("Cannot apply a RETURN BEFORE on a delete from index");
      }

    } else {
      handleTarget(result, ctx, this.fromClause, this.whereClause);
      handleLimit(result, this.limit);
    }
    handleCastToVertex(result);
    handleDelete(result);
    handleReturn(result, this.returnBefore);
    return result;
  }

  private boolean handleIndexAsTarget(
      ODeleteExecutionPlan result,
      OIndexIdentifier indexIdentifier,
      OWhereClause whereClause,
      OCommandContext ctx) {
    if (indexIdentifier == null) {
      return false;
    }
    throw new OCommandExecutionException("DELETE VERTEX FROM INDEX is not supported");
  }

  private void handleDelete(ODeleteExecutionPlan result) {
    result.chain(new DeleteStep());
  }

  private void handleUnsafe(ODeleteExecutionPlan result, boolean unsafe) {
    if (!unsafe) {
      result.chain(new CheckSafeDeleteStep());
    }
  }

  private void handleReturn(ODeleteExecutionPlan result, boolean returnBefore) {
    if (!returnBefore) {
      result.chain(new CountStep());
    }
  }

  private void handleLimit(OUpdateExecutionPlan plan, OLimit limit) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit));
    }
  }

  private void handleCastToVertex(ODeleteExecutionPlan plan) {
    plan.chain(new CastToVertexStep());
  }

  private void handleTarget(
      OUpdateExecutionPlan result,
      OCommandContext ctx,
      OFromClause target,
      OWhereClause whereClause) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, false), ctx, ctx));
  }
}
