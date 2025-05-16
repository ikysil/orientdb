package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.sql.executor.OResult;

/** Created by luigidellaquila on 03/01/17. */
public class OSQLFunctionBoth extends OSQLFunctionMove {
  public static final String NAME = "both";

  public OSQLFunctionBoth() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(final ODatabaseSession graph, final OResult rec, final String[] iLabels) {
    return v2v(graph, rec, ODirection.BOTH, iLabels);
  }
}
