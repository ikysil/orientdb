package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OCreateVertexStatement;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.ArrayList;
import java.util.List;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateVertexExecutionPlanner extends OInsertExecutionPlanner {

  public OCreateVertexExecutionPlanner(OCreateVertexStatement statement) {
    this.targetClass =
        statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName =
        statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.targetCluster =
        statement.getTargetCluster() == null ? null : statement.getTargetCluster().copy();
    if (this.targetClass == null && this.targetCluster == null && this.targetClusterName == null) {
      this.targetClass = new OIdentifier("V");
    }
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement =
        statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
  }

  @Override
  public OInsertExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OInsertExecutionPlan prev = super.createExecutionPlan(ctx);
    List<OExecutionStepInternal> steps = new ArrayList<>(prev.getSteps());
    OInsertExecutionPlan result = new OInsertExecutionPlan();

    handleCheckType(result);
    for (OExecutionStepInternal step : steps) {
      result.chain(step);
    }
    return result;
  }

  private void handleCheckType(OInsertExecutionPlan result) {
    if (targetClass != null) {
      result.chain(new CheckClassTypeStep(targetClass.getStringValue(), "V"));
    }
    if (targetClusterName != null) {
      result.chain(new CheckClusterTypeStep(targetClusterName.getStringValue(), "V"));
    }
    if (targetCluster != null) {
      result.chain(new CheckClusterTypeStep(targetCluster, "V"));
    }
  }
}
