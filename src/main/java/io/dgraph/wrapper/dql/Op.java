package io.dgraph.wrapper.dql;

public enum Op {
  uid(1),
  le(1),
  lt(2),
  eq(2),
  ge(2),
  gt(2),
  has(1);

  /** @param memberCount */
  Op(int memberCount) {
    this.memberCount = memberCount;
  }

  /** */
  private int memberCount;

  /**
   * @param key
   * @param values
   * @return
   */
  public String toDql(String key, Object... values) {
    if (uid.equals(this)) {
      return String.format(
          "%s(%s)", this.name(), (null != values && values.length > 0) ? values[0] : "");
    }
    switch (memberCount) {
      case 1:
        return this.name() + "(" + key + ")";
      case 2:
        String value = "";
        if (null != values && values.length > 0) {
          value =
              (values[0] instanceof String)
                  ? (String.format("\"%s\"", values[0]))
                  : ("" + values[0]);
        }
        return String.format("%s(%s,%s)", this.name(), key, value);
    }

    // TODO how about multivariate operator(memberCount>=3)
    return "";
  }
}
