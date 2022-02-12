package io.dgraph.wrapper.dql;

public class NotCondition extends SimpleCondition {
  public NotCondition(String key, Op op, String value) {
    super(key, op, value, true);
  }
}
