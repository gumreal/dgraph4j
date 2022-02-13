package io.dgraph.wrapper.dql;

public enum Join {
  AND,
  OR;

  public String joinStr() {
    return " " + this.name() + " ";
  }
}
