package io.dgraph.wrapper.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SimpleEdge implements Serializable {
  String fromUid;
  String edgeType;
  String toUid;
  Map<String, Object> facets;

  public SimpleEdge() {}

  public SimpleEdge(String from, String edgeType, String to) {
    this(from, edgeType, to, new HashMap<>());
  }

  public SimpleEdge(String from, String edgeType, String to, Map<String, Object> facets) {
    setFromUid(from);
    setEdgeType(edgeType);
    setToUid(to);
    setFacets(facets);
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

  public Map<String, Object> getFacets() {
    return facets;
  }

  public void setFacets(Map<String, Object> facets) {
    this.facets = facets;
  }
}
