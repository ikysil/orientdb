package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

public class OIndexSearchInfo {

  private boolean allowsRangeQueries;
  private boolean map;
  private boolean indexByKey;
  private String field;
  private OCommandContext ctx;
  private boolean indexByValue;
  private boolean supportNull;

  public OIndexSearchInfo(
      String indexField,
      boolean allowsRangeQueries,
      boolean map,
      boolean indexByKey,
      boolean indexByValue,
      boolean supportNull,
      OCommandContext ctx) {
    this.field = indexField;
    this.allowsRangeQueries = allowsRangeQueries;
    this.map = map;
    this.indexByKey = indexByKey;
    this.ctx = ctx;
    this.indexByValue = indexByValue;
    this.supportNull = supportNull;
  }

  public String getField() {
    return field;
  }

  public OCommandContext getCtx() {
    return ctx;
  }

  public boolean allowsRange() {
    return allowsRangeQueries;
  }

  public boolean isMap() {
    return map;
  }

  public boolean isIndexByKey() {
    return indexByKey;
  }

  public boolean isIndexByValue() {
    return indexByValue;
  }

  public boolean isSupportNull() {
    return supportNull;
  }
}
