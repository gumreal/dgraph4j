package io.dgraph.wrapper.dql;

import io.dgraph.wrapper.GeneralHelper;
import io.dgraph.wrapper.model.DataType;

public class SimpleCondition {
  public static final int EMPTY_VALUE_AS_NOT_EXP = 1; // null value, means: NOT has(key)
  public static final int EMPTY_VALUE_KEEP = 2;
  public static final int EMPTY_VALUE_SKIP = 3;

  boolean notLogic;
  String key;
  Op op;
  Object value;

  public SimpleCondition(String key, Op op, Object obj) {
    this(key, op, obj, false);
  }

  protected SimpleCondition(String key, Op op, Object obj, boolean notLogic) {
    this.key = key;
    this.op = op;
    this.value = obj;
    this.notLogic = notLogic;
  }

  /**
   * @param uid
   * @return
   */
  public static SimpleCondition UID(String uid) {
    return new SimpleCondition(DataType.DT_UID.toString(), Op.uid, uid);
  }

  public String toDql() {
    return toDql(false, EMPTY_VALUE_AS_NOT_EXP);
  }

  public String toDql(boolean asFunc) {
    return toDql(asFunc, EMPTY_VALUE_AS_NOT_EXP);
  }

  public String toDql(int emptyValueMode) {
    return toDql(false, emptyValueMode);
  }

  /** @return */
  public String toDql(boolean asFunc, int emptyValueMode) {
    // TODO finish this
    boolean isEmpty = false;
    if (null == value) {
      isEmpty = true;
    } else {
      if (value instanceof String) {
        isEmpty = GeneralHelper.isEmpty((String) value);
      }
    }

    StringBuffer buffer = new StringBuffer();
    if (isEmpty) {
      switch (emptyValueMode) {
        case EMPTY_VALUE_KEEP:
          value = "";
          break;

        case EMPTY_VALUE_AS_NOT_EXP:
          String temp = "NOT " + Op.has.name() + "(" + key + ")";
          temp = isNotLogic() ? GeneralHelper.wrapDqlNot(temp) : temp;
          return asFunc ? "func:(" + temp + ")" : temp;

        default:
          return "";
      }
    }

    return String.format("%s%s", asFunc ? "func:" : "", op.toDql(key, value));
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
