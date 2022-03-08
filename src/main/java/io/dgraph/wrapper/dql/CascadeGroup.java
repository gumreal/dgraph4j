package io.dgraph.wrapper.dql;

import io.dgraph.wrapper.GeneralHelper;
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
    this(notLogic, Join.AND);
  }

  public CascadeGroup(Join join) {
    this(false, join);
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

  /** @return */
  public boolean hasExpression() {
    return simpleGroups.size() > 0 || cascadeGroups.size() > 0;
  }

  /**
   * Generate DQL Filter expression
   *
   * @return
   */
  public String toDql() {
    StringBuffer buffer = new StringBuffer();

    int expCount = 0;
    for (SimpleGroup sGroup : simpleGroups) {
      if (!sGroup.hasExpression()) {
        continue;
      }
      if (expCount++ > 0) {
        buffer.append(join.joinStr());
      }
      buffer.append("(" + sGroup.toDql() + ")");
    }
    for (CascadeGroup cGroup : cascadeGroups) {
      if (!cGroup.hasExpression()) {
        continue;
      }
      if (expCount++ > 0) {
        buffer.append(join.joinStr());
      }
      buffer.append("(" + cGroup.toDql() + ")");
    }

    // done
    return notLogic ? GeneralHelper.wrapDqlNot(buffer.toString()) : buffer.toString();
  }

  public String toString() {
    return toDql();
  }

  public List<SimpleGroup> getSimpleGroups() {
    return simpleGroups;
  }

  public List<CascadeGroup> getCascadeGroups() {
    return cascadeGroups;
  }
}
