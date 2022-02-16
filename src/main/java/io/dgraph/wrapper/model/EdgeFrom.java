package io.dgraph.wrapper.model;

/**
 * represent the from vertex of an edge, with facets map of the edge
 *
 * @param <T> the end vertex type, which must extend the VertexBase class
 */
public class EdgeFrom<T extends VertexBase> extends EdgeTo<T> {}
