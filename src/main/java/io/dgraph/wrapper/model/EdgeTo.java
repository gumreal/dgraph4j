package io.dgraph.wrapper.model;

import java.util.HashMap;
import java.util.Map;

public class EdgeTo<T extends VertexBase> {
  private T toV;
  private Map<String, Object> facets;

  public EdgeTo() {}

  public EdgeTo(T toV) {
    this.toV = toV;
  }

  public EdgeTo<T> withFacet(String k, String v) {
    if (null == facets) {
      facets = new HashMap<>();
    }
    facets.put(k, v);
    return this;
  }

  public EdgeTo<T> withFacet(String k, int v) {
    if (null == facets) {
      facets = new HashMap<>();
    }
    facets.put(k, v);
    return this;
  }

  public EdgeTo<T> withFacet(String k, float v) {
    if (null == facets) {
      facets = new HashMap<>();
    }
    facets.put(k, v);
    return this;
  }

  public T getToV() {
    return toV;
  }

  public void setToV(T toV) {
    this.toV = toV;
  }

  public Map<String, Object> getFacets() {
    return facets;
  }

  public void setFacets(Map<String, Object> facets) {
    this.facets = facets;
  }
}
