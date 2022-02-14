package io.dgraph.wrapper.model;

/**
 * an edge, with its start vertex, end vertex, edge type, and facets
 *
 * @param <F> the start vertex type, which must extend the VertexBase class
 * @param <T> the end vertex type, which must extend the VertexBase class
 */
public class Edge<F extends VertexBase, T extends VertexBase> extends EdgeTo<T> {
  private F fromV;
  private String edgeType;

  public Edge() {}

  public Edge(F fromV, T toV, String edgeType) {
    super(toV);

    setFromV(fromV);
    setEdgeType(edgeType);
  }

  public F getFromV() {
    return fromV;
  }

  public void setFromV(F fromV) {
    this.fromV = fromV;
  }

  public String getEdgeType() {
    return edgeType;
  }

  public void setEdgeType(String edgeType) {
    this.edgeType = edgeType;
  }
}
