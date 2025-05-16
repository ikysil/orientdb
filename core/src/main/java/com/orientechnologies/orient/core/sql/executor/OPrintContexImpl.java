package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OStepStats;
import java.text.DecimalFormat;

public class OPrintContexImpl implements OPrintContext {

  private int depth = 0;
  private int ident = 0;
  private OCommandContext ctx;

  public OPrintContexImpl() {}

  public OPrintContexImpl(OCommandContext ctx) {
    this.ctx = ctx;
  }

  public OPrintContexImpl(OCommandContext ctx, int depth, int ident) {
    this.ctx = ctx;
    this.depth = depth;
    this.ident = ident;
  }

  @Override
  public int getDepth() {
    return depth;
  }

  @Override
  public int getIdent() {
    return ident;
  }

  @Override
  public void incDepth() {
    depth++;
  }

  @Override
  public void decDepth() {
    depth--;
  }

  public long getCost(OExecutionStepInternal step) {
    if (ctx != null) {
      OStepStats stats = this.ctx.getStats(step);
      if (stats != null) {
        return stats.getCost();
      } else {
        return -1l;
      }
    } else {
      return -1l;
    }
  }

  public String getCostFormatted(OExecutionStepInternal step) {
    if (ctx != null) {
      return new DecimalFormat().format(getCost(step) / 1000) + "μs";
    } else {
      return "";
    }
  }

  @Override
  public boolean isProfilingEnabled() {
    if (ctx != null) {
      return ctx.isProfiling();
    }
    return false;
  }
}
