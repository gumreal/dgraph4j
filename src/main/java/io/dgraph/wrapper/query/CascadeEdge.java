package io.dgraph.wrapper.query;

import io.dgraph.wrapper.model.VertexBase;

/** */
public class CascadeEdge {
  private String edgeType;
  private VertexBase otherVertx;
  private boolean reverse = false;

  public CascadeEdge(String edgeType, VertexBase toVertx) {
    this(edgeType, toVertx, false);
  }

  public CascadeEdge(String edgeType, VertexBase toVertx, boolean reverse) {
    setEdgeType(edgeType);
    setOtherVertx(toVertx);
    setReverse(reverse);
  }

  public String getEdgeType() {
    return edgeType;
  }

  public void setEdgeType(String edgeType) {
    this.edgeType = edgeType;
  }

  public VertexBase getOtherVertx() {
    return otherVertx;
  }

  public void setOtherVertx(VertexBase otherVertx) {
    this.otherVertx = otherVertx;
  }

  public boolean isReverse() {
    return reverse;
  }

  public void setReverse(boolean reverse) {
    this.reverse = reverse;
  }
}
