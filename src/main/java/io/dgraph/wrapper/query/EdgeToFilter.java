package io.dgraph.wrapper.query;

import io.dgraph.wrapper.model.VertxBase;

/** */
public class EdgeToFilter {
  private String edgeType;
  private VertxBase toVertx;
  private boolean reverse = false;

  public EdgeToFilter(String edgeType, VertxBase toVertx) {
    this(edgeType, toVertx, false);
  }

  public EdgeToFilter(String edgeType, VertxBase toVertx, boolean reverse) {
    setEdgeType(edgeType);
    setToVertx(toVertx);
    setReverse(reverse);
  }

  public String getEdgeType() {
    return edgeType;
  }

  public void setEdgeType(String edgeType) {
    this.edgeType = edgeType;
  }

  public VertxBase getToVertx() {
    return toVertx;
  }

  public void setToVertx(VertxBase toVertx) {
    this.toVertx = toVertx;
  }

  public boolean isReverse() {
    return reverse;
  }

  public void setReverse(boolean reverse) {
    this.reverse = reverse;
  }
}
