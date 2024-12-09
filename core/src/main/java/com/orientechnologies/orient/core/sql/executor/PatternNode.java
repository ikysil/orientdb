package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import java.util.LinkedHashSet;
import java.util.Set;

/** Created by luigidellaquila on 28/07/15. */
public class PatternNode {
  private final String alias;
  private final Set<PatternEdge> out = new LinkedHashSet<PatternEdge>();
  private final Set<PatternEdge> in = new LinkedHashSet<PatternEdge>();
  private int centrality = 0;
  private boolean optional = false;

  public PatternNode(String alias) {
    this.alias = alias;
  }

  public int addEdge(OMatchPathItem item, PatternNode to) {
    PatternEdge edge = new PatternEdge(item, to, this);
    this.getOut().add(edge);
    to.getIn().add(edge);
    return 1;
  }

  public boolean isOptionalNode() {
    return isOptional();
  }

  public boolean isOptional() {
    return optional;
  }

  public void setOptional(boolean optional) {
    this.optional = optional;
  }

  public int getCentrality() {
    return centrality;
  }

  public void setCentrality(int centrality) {
    this.centrality = centrality;
  }

  public Set<PatternEdge> getIn() {
    return in;
  }

  public Set<PatternEdge> getOut() {
    return out;
  }

  public String getAlias() {
    return alias;
  }
}
