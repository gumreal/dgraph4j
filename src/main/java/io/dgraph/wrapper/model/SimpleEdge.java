package io.dgraph.wrapper.model;

import java.io.Serializable;

public class SimpleEdge implements Serializable {
  String fromUid;
  String edgeType;
  String toUid;

  public SimpleEdge() {}

  public SimpleEdge(String from, String edgeType, String to) {
    setFromUid(from);
    setEdgeType(edgeType);
    setToUid(to);
  }

  /**
   * json string for creating the edge
   *
   * @return
   */
  public String toLinkJson() {
    return String.format(linkTemplate, fromUid, edgeType, toUid);
  }

  private static String linkTemplate =
      "{\n "
          + "   \"uid\":\"%s\",\n "
          + "   \"%s\":{\n "
          + "       \"uid\":\"%s\"\n "
          + "   }\n "
          + "}\n";

  public String getFromUid() {
    return fromUid;
  }

  public void setFromUid(String fromUid) {
    this.fromUid = fromUid;
  }

  public String getEdgeType() {
    return edgeType;
  }

  public void setEdgeType(String edgeType) {
    this.edgeType = edgeType;
  }

  public String getToUid() {
    return toUid;
  }

  public void setToUid(String toUid) {
    this.toUid = toUid;
  }
}
