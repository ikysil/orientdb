package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public interface OMapExecutionStream {

  OExecutionStream flatMap(OResult next, OCommandContext ctx);
}
