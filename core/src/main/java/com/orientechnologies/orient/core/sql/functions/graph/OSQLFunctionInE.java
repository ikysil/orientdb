package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.sql.executor.OResult;

/** Created by luigidellaquila on 03/01/17. */
public class OSQLFunctionInE extends OSQLFunctionMove {
  public static final String NAME = "inE";

  public OSQLFunctionInE() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final ODatabaseSession graph, final OResult iRecord, final String[] iLabels) {
    return v2e(graph, iRecord, ODirection.IN, iLabels);
  }
}
