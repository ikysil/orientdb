package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OStepStats;

public class OToResultContextImpl implements OToResultContext {

  private OCommandContext context;

  public OToResultContextImpl() {}

  public OToResultContextImpl(OCommandContext context) {
    this.context = context;
  }

  @Override
  public long getCost(OExecutionStepInternal step) {
    if (context != null) {
      OStepStats stats = context.getStats(step);
      if (stats != null) {
        return stats.getCost();
      } else {
        return -1;
      }
    } else {
      return -1;
    }
  }

  @Override
  public OCommandContext getContext() {
    return context;
  }
}
