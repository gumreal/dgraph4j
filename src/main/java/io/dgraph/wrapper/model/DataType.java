package io.dgraph.wrapper.model;

/** */
public enum DataType {
  DT_BOOL("bool"),
  DT_DATETIME("dateTime"),
  DT_fLOAT("float"),
  DT_GEO("geo"),
  DT_INT("int"),
  DT_STRING("string"),
  DT_UID("uid"),
  DT_DGRAPH_TYPE("dgraph.type");

  private String dgraphType;

  DataType(String dgt) {
    this.dgraphType = dgt;
  }

  @Override
  public String toString() {
    return dgraphType;
  }
}
