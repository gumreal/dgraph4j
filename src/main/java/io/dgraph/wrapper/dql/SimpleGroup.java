package io.dgraph.wrapper.dql;

import io.dgraph.wrapper.GeneralHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** */
public abstract class SimpleGroup {
  private boolean notLogic;
  private Join join;
  private List<SimpleCondition> simpleConditions = new ArrayList<>();

  public SimpleGroup() {
    this(false, Join.AND);
  }

  public SimpleGroup(boolean notLogic, Join join) {
    this.notLogic = notLogic;
    this.join = join;
  }

  public SimpleGroup withConditions(SimpleCondition... cond) {
    for (int i = 0; i < cond.length; i++) {
      simpleConditions.add(cond[i]);
    }
    return this;
  }

  public SimpleGroup withConditions(Op op, Map<String, Object> map) {
    // add map conditions
    map.forEach((k, v) -> simpleConditions.add(new SimpleCondition(k, op, v)));
    return this;
  }

  public boolean hasExpression() {
    return simpleConditions.size() > 0;
  }

  /** @return */
  public String toDql() {
    StringBuffer buffer = new StringBuffer();
    int expCount = 0;
    for (SimpleCondition cond : simpleConditions) {
      if (expCount++ > 0) {
        buffer.append(join.joinStr());
      }
      buffer.append(cond.toDql());
    }

    // done
    return notLogic ? GeneralHelper.wrapDqlNot(buffer.toString()) : buffer.toString();
  }

  public String toString() {
    return toDql();
  }
}
