package io.dgraph.wrapper.dql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class SimpleGroup {
  boolean notLogic;
  Join join;
  List<SimpleCondition> simpleConditions = new ArrayList<>();

  public SimpleGroup() {
    this(false, Join.and);
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
}
