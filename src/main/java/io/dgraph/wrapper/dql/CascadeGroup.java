package io.dgraph.wrapper.dql;

import java.util.ArrayList;
import java.util.List;

public class CascadeGroup {
  boolean notLogic;
  Join join;
  List<SimpleGroup> simpleGroups = new ArrayList<>();
  List<CascadeGroup> cascadeGroups = new ArrayList<>();

  public CascadeGroup() {
    this(false);
  }

  public CascadeGroup(boolean notLogic) {
    this(notLogic, Join.and);
  }

  public CascadeGroup(boolean notLogic, Join join) {
    this.notLogic = notLogic;
    this.join = join;
  }

  public CascadeGroup withGroup(SimpleGroup group) {
    this.simpleGroups.add(group);
    return this;
  }

  public CascadeGroup withGroups(SimpleGroup... groups) {
    for (int i = 0; i < groups.length; i++) {
      this.simpleGroups.add(groups[i]);
    }
    return this;
  }

  public CascadeGroup withGroup(CascadeGroup group) {
    this.cascadeGroups.add(group);
    return this;
  }

  public CascadeGroup withGroups(CascadeGroup... groups) {
    for (int i = 0; i < groups.length; i++) {
      this.cascadeGroups.add(groups[i]);
    }
    return this;
  }

  /**
   * Generate DQL Filter expression
   *
   * @return
   */
  public String dqlFilter() {
    // TODO finish this
    return "";
  }

  public List<CascadeGroup> getCascadeGroups() {
    return cascadeGroups;
  }
}
