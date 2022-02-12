package io.dgraph.wrapper.dql;

public class SimpleCondition {
  boolean notLogic;
  String key;
  Op op;
  Object value; // null value, means: NOT key

  public SimpleCondition(String key, Op op, Object obj) {
    this(key, op, obj, false);
  }

  protected SimpleCondition(String key, Op op, Object obj, boolean notLogic) {
    this.key = key;
    this.op = op;
    this.value = obj;
    this.notLogic = notLogic;
  }

  /** @return */
  public String dqlFunc() {
    // TODO finish this
    return String.format(
        "func:%s(%s,%s)",
        op.name(),
        key,
        (value instanceof String) ? (String.format("\"%s\"", value)) : ("" + value));
  }

  public boolean isNotLogic() {
    return notLogic;
  }

  public String getKey() {
    return key;
  }

  public Op getOp() {
    return op;
  }

  public Object getValue() {
    return value;
  }
}
