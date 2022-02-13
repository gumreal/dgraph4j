package io.dgraph.wrapper.dql;

import java.util.Map;

public class AndGroup extends SimpleGroup {
  public AndGroup() {
    this(false);
  }

  public AndGroup(boolean notLogic) {
    super(notLogic, Join.AND);
  }

  public static SimpleGroup create(SimpleCondition... cond) {
    return new AndGroup().withConditions(cond);
  }

  public static SimpleGroup create(Op op, Map<String, Object> map) {
    return new AndGroup().withConditions(op, map);
  }
}
