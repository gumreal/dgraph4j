package io.dgraph.wrapper.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * represent the end vertex of an edge, with facets map of the edge
 *
 * @param <T> the end vertex type, which must extend the VertexBase class
 */
public class EdgeTo<T extends VertexBase> implements Serializable {
  private T vertex;

  /** the facets map */
  private Map<String, Object> facets;

  public EdgeTo() {}

  public EdgeTo(T vertex) {
    this.vertex = vertex;
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

  /**
   * @param k
   * @return
   */
  public Object getFacet(String k) {
    if (null == facets || !facets.containsKey(k)) {
      return null;
    }
    return facets.get(k);
  }

  public T getVertex() {
    return vertex;
  }

  public void setVertex(T vertex) {
    this.vertex = vertex;
  }

  public Map<String, Object> getFacets() {
    return facets;
  }

  public void setFacets(Map<String, Object> facets) {
    this.facets = facets;
  }
}
