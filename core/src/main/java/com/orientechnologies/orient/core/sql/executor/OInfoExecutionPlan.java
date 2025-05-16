package com.orientechnologies.orient.core.sql.executor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Created by luigidellaquila on 19/12/16. */
public class OInfoExecutionPlan implements OExecutionPlan {

  private List<OExecutionStep> steps = new ArrayList<>();
  private String prettyPrint;
  private String type;
  private String javaType;
  private Integer cost;
  private String stmText;
  private String genericStatement;
  private OResult result;

  @Override
  public List<OExecutionStep> getSteps() {
    return steps;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return prettyPrint;
  }

  public String prettyPrint() {
    return prettyPrint;
  }

  @Override
  public OResult toResult() {
    return result;
  }

  public void setSteps(List<OExecutionStep> steps) {
    this.steps = steps;
  }

  public String getPrettyPrint() {
    return prettyPrint;
  }

  public void setPrettyPrint(String prettyPrint) {
    this.prettyPrint = prettyPrint;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getJavaType() {
    return javaType;
  }

  public void setJavaType(String javaType) {
    this.javaType = javaType;
  }

  public Integer getCost() {
    return cost;
  }

  public void setCost(Integer cost) {
    this.cost = cost;
  }

  public String getStmText() {
    return stmText;
  }

  public void setStmText(String stmText) {
    this.stmText = stmText;
  }

  @Override
  public String toString() {
    return prettyPrint;
  }

  @Override
  public Set<String> getIndexes() {
    Set<String> indexes = new HashSet<>();
    for (OExecutionStep chilStep : steps) {
      fillIndexes(chilStep, indexes);
    }
    return indexes;
  }

  static void fillIndexes(OExecutionStep step, Set<String> indexes) {
    for (OExecutionStep chilStep : step.getSubSteps()) {
      fillIndexes(chilStep, indexes);
    }
    String index = step.toResult().getProperty("index");
    if (index != null) {
      indexes.add(index);
    }
  }

  public String getGenericStatement() {
    return genericStatement;
  }

  public void setGenericStatement(String genericStatement) {
    this.genericStatement = genericStatement;
  }

  public static OInfoExecutionPlan fromResult(OResult read) {
    OInfoExecutionPlan result = new OInfoExecutionPlan();
    result.result = read;
    result.setCost(((Number) read.getProperty("cost")).intValue());
    result.setType(read.getProperty("type"));
    result.setJavaType(read.getProperty("javaType"));
    result.setPrettyPrint(read.getProperty("prettyPrint"));
    result.setStmText(read.getProperty("stmText"));
    result.setGenericStatement(read.getProperty("genericStm"));
    List<OResult> subSteps = read.getProperty("steps");
    if (subSteps != null) {
      subSteps.forEach(x -> result.getSteps().add(OInfoExecutionStep.fromResult(x)));
    }
    return result;
  }
}
