package com.orientechnologies.orient.core.sql.executor;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public interface OTodoResultSet {

  public boolean hasNext();

  public OResult next();

  void close();

  Optional<OExecutionPlan> getExecutionPlan();

  Map<String, Object> getQueryStats();

}
