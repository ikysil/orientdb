package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlanContextOps;
import com.orientechnologies.orient.core.sql.executor.OPrintContexImpl;
import com.orientechnologies.orient.core.sql.executor.OPrintContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OToResultContext;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;

/** Created by Enrico Risa on 25/05/2017. */
public class OGremlinExecutionPlan implements OExecutionPlanContextOps {

  TraversalExplanation explanation;

  public OGremlinExecutionPlan(TraversalExplanation explanation) {
    this.explanation = explanation;
  }

  public String prettyPrint(OPrintContext ctx) {
    return explanation.prettyPrint();
  }

  @Override
  public OResult toResult(OToResultContext ctx) {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "GremlinExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("stmText", null);
    result.setProperty("cost", null);
    result.setProperty("prettyPrint", prettyPrint(new OPrintContexImpl(ctx.getContext(), 0, 2)));

    return result;
  }
}
