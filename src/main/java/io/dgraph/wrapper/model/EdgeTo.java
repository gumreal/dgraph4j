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

  /**
   * merge other to this
   *
   * @param other
   * @return
   */
  public boolean merge(EdgeTo<T> other) {
    if (null == other || null == other.getVertex()) {
      return false;
    }

    T otherVertex = other.getVertex();
    if (null == getVertex()) {
      setVertex(otherVertex);
    } else {
      getVertex().merge(otherVertex);
    }

    mergeFacets(other.getFacets());
    return true;
  }

  /** @param map */
  protected void mergeFacets(Map<String, Object> map) {
    if (null == getFacets()) {
      setFacets(map);
      return;
    }
    map.entrySet()
        .forEach(
            entry -> {
              Object v = entry.getValue();
              if (v instanceof Integer) {
                Integer vInt = (Integer) v;
                if (!getFacets().containsKey(entry.getKey())) {
                  withFacet(entry.getKey(), vInt);
                } else {
                  Object oldV = getFacet(entry.getKey());
                  withFacet(
                      entry.getKey(),
                      vInt.intValue()
                          + ((null != oldV && oldV instanceof Integer)
                              ? ((Integer) oldV).intValue()
                              : 0));
                }
              } else if (v instanceof Float) {
                Float vFloat = (Float) v;
                if (!getFacets().containsKey(entry.getKey())) {
                  withFacet(entry.getKey(), vFloat);
                } else {
                  Object oldV = getFacet(entry.getKey());
                  withFacet(
                      entry.getKey(),
                      vFloat.floatValue()
                          + ((null != oldV && oldV instanceof Float)
                              ? ((Float) oldV).floatValue()
                              : 0.0f));
                }
              } else {
                // treat v as String
                String vStr = (String) v;
                if (!getFacets().containsKey(entry.getKey())) {
                  withFacet(entry.getKey(), vStr);
                } else {
                  Object oldV = getFacet(entry.getKey());
                  withFacet(entry.getKey(), vStr + ((null != oldV) ? (", " + oldV) : ""));
                }
              }
            });
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
