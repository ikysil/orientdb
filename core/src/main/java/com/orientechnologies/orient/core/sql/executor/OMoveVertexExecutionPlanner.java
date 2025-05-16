package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OBatch;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OFromItem;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OMoveVertexStatement;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OUpdateOperations;

/** Created by luigidellaquila on 08/08/16. */
public class OMoveVertexExecutionPlanner {
  private final OFromItem source;
  private final OIdentifier targetClass;
  private final OCluster targetCluster;
  private final OUpdateOperations updateOperations;
  private final OBatch batch;

  public OMoveVertexExecutionPlanner(OMoveVertexStatement oStatement) {
    this.source = oStatement.getSource();
    this.targetClass = oStatement.getTargetClass();
    this.targetCluster = oStatement.getTargetCluster();
    this.updateOperations = oStatement.getUpdateOperations();
    this.batch = oStatement.getBatch();
  }

  public OUpdateExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OUpdateExecutionPlan result = new OUpdateExecutionPlan();

    handleSource(result, ctx, this.source);
    convertToModifiableResult(result);
    handleTarget(result, targetClass, targetCluster, ctx);
    handleOperations(result, this.updateOperations);
    handleBatch(result, ctx, this.batch);
    handleSave(result);
    return result;
  }

  private void handleTarget(
      OUpdateExecutionPlan result,
      OIdentifier targetClass,
      OCluster targetCluster,
      OCommandContext ctx) {
    result.chain(new MoveVertexStep(targetClass, targetCluster, ctx));
  }

  private void handleBatch(OUpdateExecutionPlan result, OCommandContext ctx, OBatch batch) {
    if (batch != null) {
      result.chain(new BatchStep(batch, ctx));
    }
  }

  /**
   * add a step that transforms a normal OResult in a specific object that under setProperty()
   * updates the actual OIdentifiable
   *
   * @param plan the execution plan
   */
  private void convertToModifiableResult(OUpdateExecutionPlan plan) {
    plan.chain(new ConvertToUpdatableResultStep());
  }

  private void handleSave(OUpdateExecutionPlan result) {
    result.chain(new SaveElementStep());
  }

  private void handleOperations(OUpdateExecutionPlan plan, OUpdateOperations op) {
    if (op != null) {
      switch (op.getType()) {
        case OUpdateOperations.TYPE_SET:
          plan.chain(new UpdateSetStep(op.getUpdateItems()));
          break;
        case OUpdateOperations.TYPE_REMOVE:
          plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems()));
          break;
        case OUpdateOperations.TYPE_MERGE:
          plan.chain(new UpdateMergeStep(op.getJson()));
          break;
        case OUpdateOperations.TYPE_CONTENT:
          plan.chain(new UpdateContentStep(op.getJson()));
          break;
        case OUpdateOperations.TYPE_PUT:
        case OUpdateOperations.TYPE_INCREMENT:
        case OUpdateOperations.TYPE_ADD:
          throw new OCommandExecutionException(
              "Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
      }
    }
  }

  private void handleSource(OUpdateExecutionPlan result, OCommandContext ctx, OFromItem source) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(new OFromClause(-1));
    sourceStatement.getTarget().setItem(source);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, false), ctx, ctx));
  }
}
