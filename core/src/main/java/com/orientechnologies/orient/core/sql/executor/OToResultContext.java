package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

public interface OToResultContext {

  long getCost(OExecutionStepInternal step);

  OCommandContext getContext();
}
