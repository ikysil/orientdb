/* Generated By:JJTree: Do not edit this line. OProfileStorageStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Map;

public class OProfileStorageStatement extends OSimpleExecStatement {

  protected boolean on;

  public static final String KEYWORD_PROFILE = "PROFILE";

  public OProfileStorageStatement(int id) {
    super(id);
  }

  public OProfileStorageStatement(OrientSql p, int id) {
    super(p, id);
  }

  // new execution logic
  @Override
  public OExecutionStream executeSimple(OCommandContext ctx) {
    OResultInternal result = new OResultInternal();
    result.setProperty("operation", "optimize database");

    return OExecutionStream.singleton(result);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("PROFILE STORAGE ");
    builder.append(on ? "ON" : "OFF");
  }

  public void toGenericStatement(StringBuilder builder) {
    builder.append("PROFILE STORAGE ");
    builder.append(on ? "ON" : "OFF");
  }

  @Override
  public OProfileStorageStatement copy() {
    OProfileStorageStatement result = new OProfileStorageStatement(-1);
    result.on = on;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OProfileStorageStatement that = (OProfileStorageStatement) o;

    return on == that.on;
  }

  @Override
  public int hashCode() {
    return (on ? 1 : 0);
  }
}

/* JavaCC - OriginalChecksum=645887712797ae14a17820bfa944f78e (do not edit this line) */
