package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMatchStatement;

/** Created by luigidellaquila on 28/07/15. */
public class PatternEdge {
  private final PatternNode in;
  private final PatternNode out;
  private final OMatchPathItem item;

  public PatternEdge(OMatchPathItem item, PatternNode in, PatternNode out) {
    this.item = item;
    this.in = in;
    this.out = out;
  }

  public Iterable<OIdentifiable> executeTraversal(
      OMatchStatement.MatchContext matchContext,
      OCommandContext iCommandContext,
      OIdentifiable startingPoint,
      int depth) {
    return getItem().executeTraversal(matchContext, iCommandContext, startingPoint, depth);
  }

  @Override
  public String toString() {
    return "{as: "
        + getOut().getAlias()
        + "}"
        + getItem().toString()
        + "{as: "
        + getIn().getAlias()
        + "}";
  }

  public OMatchPathItem getItem() {
    return item;
  }

  public PatternNode getOut() {
    return out;
  }

  public PatternNode getIn() {
    return in;
  }
}
