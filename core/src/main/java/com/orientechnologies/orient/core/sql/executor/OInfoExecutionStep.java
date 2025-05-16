package com.orientechnologies.orient.core.sql.executor;

import java.util.ArrayList;
import java.util.List;

/** Created by luigidellaquila on 19/12/16. */
public class OInfoExecutionStep implements OExecutionStep {

  private String name;
  private String type;
  private String javaType;
  private String targetNode;
  private String description;
  private long cost;
  private List<OExecutionStep> subSteps = new ArrayList<>();
  private List<OExecutionPlan> subPlans = new ArrayList<>();
  private OResult sourceResult;

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public String getTargetNode() {
    return targetNode;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public List<OExecutionStep> getSubSteps() {
    return subSteps;
  }

  public List<OExecutionPlan> getSubExecutionPlans() {
    return subPlans;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public OResult toResult() {
    return sourceResult;
  }

  public void setSourceResult(OResult sourceResult) {
    this.sourceResult = sourceResult;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setTargetNode(String targetNode) {
    this.targetNode = targetNode;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setCost(long cost) {
    this.cost = cost;
  }

  public String getJavaType() {
    return javaType;
  }

  public void setJavaType(String javaType) {
    this.javaType = javaType;
  }

  protected static OExecutionStep fromResult(OResult x) {
    OInfoExecutionStep result = new OInfoExecutionStep();
    result.setSourceResult(x);
    result.setName(x.getProperty("name"));
    result.setType(x.getProperty("type"));
    result.setTargetNode(x.getProperty("targetNode"));
    result.setJavaType(x.getProperty("javaType"));
    result.setCost(x.getProperty("cost") == null ? -1 : x.getProperty("cost"));
    List<OResult> ssteps = x.getProperty("subSteps");
    if (ssteps != null) {
      ssteps.stream()
          .forEach(sstep -> result.getSubSteps().add(OInfoExecutionStep.fromResult(sstep)));
    }
    List<OResult> splans = x.getProperty("subExecutionPlans");
    if (splans != null) {
      splans.stream()
          .forEach(
              splan -> result.getSubExecutionPlans().add(OInfoExecutionPlan.fromResult(splan)));
    }
    result.setDescription(x.getProperty("description"));
    return result;
  }
}
