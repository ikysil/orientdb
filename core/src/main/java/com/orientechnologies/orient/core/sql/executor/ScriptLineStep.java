package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OReturnStatement;
import com.orientechnologies.orient.core.sql.parser.OStatement;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 *     <p>This step represents the execution plan of an instruciton instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {
  protected final OStatement statement;

  public ScriptLineStep(OStatement statement, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.statement = statement;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OInternalExecutionPlan plan = statement.createExecutionPlan(ctx);
    if (plan instanceof OInsertExecutionPlan) {
      ((OInsertExecutionPlan) plan).executeInternal(ctx);
    } else if (plan instanceof ODeleteExecutionPlan) {
      ((ODeleteExecutionPlan) plan).executeInternal(ctx);
    } else if (plan instanceof OUpdateExecutionPlan) {
      ((OUpdateExecutionPlan) plan).executeInternal(ctx);
    } else if (plan instanceof ODDLExecutionPlan) {
      ((ODDLExecutionPlan) plan).executeInternal((OBasicCommandContext) ctx);
    } else if (plan instanceof OSingleOpExecutionPlan) {
      ((OSingleOpExecutionPlan) plan).executeInternal((OBasicCommandContext) ctx);
    }
    return plan.start(ctx);
  }

  public boolean containsReturn() {
    OInternalExecutionPlan plan = statement.createExecutionPlan(ctx);
    if (plan instanceof OScriptExecutionPlan) {
      return ((OScriptExecutionPlan) plan).containsReturn();
    }
    if (plan instanceof OSingleOpExecutionPlan) {
      if (((OSingleOpExecutionPlan) plan).statement instanceof OReturnStatement) {
        return true;
      }
    }
    if (plan instanceof OIfExecutionPlan) {
      if (((OIfExecutionPlan) plan).containsReturn()) {
        return true;
      }
    }

    if (plan instanceof OForEachExecutionPlan) {
      if (((OForEachExecutionPlan) plan).containsReturn()) {
        return true;
      }
    }
    return false;
  }

  public OExecutionStepInternal executeUntilReturn(OCommandContext ctx) {
    OInternalExecutionPlan plan = statement.createExecutionPlan(ctx);
    if (plan instanceof OScriptExecutionPlan) {
      return ((OScriptExecutionPlan) plan).executeUntilReturn(ctx);
    }
    if (plan instanceof OSingleOpExecutionPlan) {
      if (((OSingleOpExecutionPlan) plan).statement instanceof OReturnStatement) {
        return new ReturnStep(((OSingleOpExecutionPlan) plan).statement, ctx, profilingEnabled);
      }
    }
    if (plan instanceof OIfExecutionPlan) {
      return ((OIfExecutionPlan) plan).executeUntilReturn(ctx);
    }
    throw new IllegalStateException();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (statement == null) {
      return "Script Line";
    }
    return statement.getOriginalStatement();
  }
}
