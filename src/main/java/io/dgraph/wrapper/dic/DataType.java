package io.dgraph.wrapper.dic;

/** */
public enum DataType {
  DT_BOOL("bool"),
  DT_DATETIME("dateTime"),
  DT_fLOAT("float"),
  DT_GEO("geo"),
  DT_INT("int"),
  DT_STRING("string"),
  DT_UID("uid");

  private String dgraphType;

  DataType(String dgt) {
    this.dgraphType = dgt;
  }

  @Override
  public String toString() {
    return dgraphType;
  }
}
