package com.orientechnologies.orient.core.sql.executor;

/** Created by luigidellaquila on 17/10/16. */
public class OptionalMatchStep extends MatchStep {
  public OptionalMatchStep(EdgeTraversal edge) {
    super(edge);
  }

  @Override
  protected MatchEdgeTraverser createTraverser(OResult lastUpstreamRecord) {
    return new OptionalMatchEdgeTraverser(lastUpstreamRecord, edge);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ OPTIONAL MATCH ");
    if (edge.out) {
      result.append(" ---->\n");
    } else {
      result.append("     <----\n");
    }
    result.append(spaces);
    result.append("  ");
    result.append("{" + edge.edge.getOut().getAlias() + "}");
    result.append(edge.edge.getItem().getMethod());
    result.append("{" + edge.edge.getIn().getAlias() + "}");
    return result.toString();
  }
}
