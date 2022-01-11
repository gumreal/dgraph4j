package io.dgraph.wrapper.query;

import io.dgraph.wrapper.GeneralHelper;
import java.util.HashSet;
import java.util.Set;

/** lookup vertx by values, and find their edges */
public class VertxEdgeFilter {
  private String vertx;
  private Set<String> values = new HashSet<>();
  private String edge;

  public VertxEdgeFilter(String vertx, String value, String edge) {
    this.vertx = vertx;
    this.values.add(value);
    this.edge = edge;
  }

  public VertxEdgeFilter(String vertx, Set<String> values, String edge) {
    this.vertx = vertx;
    if (null != values) {
      this.values = values;
    }
    this.edge = edge;
  }

  public boolean isValid() {
    return !GeneralHelper.isEmpty(vertx) && !GeneralHelper.isEmpty(edge) && values.size() > 0;
  }

  public String toString() {
    return String.format(
        "[vertx]%s  [values]%s  [edge]%s",
        null == vertx ? "" : vertx, values, null == edge ? "" : edge);
  }

  private static String phrasePattern =
      "var(func:eq(%s, %s)){\n" + "   %s{\n" + "      %s as uid\n" + "   }\n" + "}\n";

  public String getQueryPhrase(String varName) {
    return String.format(
        phrasePattern, vertx, GeneralHelper.implode(values, ",", "\"", "\""), edge, varName);
  }

  public String getVertx() {
    return vertx;
  }

  public void setVertx(String vertx) {
    this.vertx = vertx;
  }

  public Set<String> getValues() {
    return values;
  }

  public void setValues(Set<String> values) {
    this.values = values;
  }

  public String getEdge() {
    return edge;
  }

  public void setEdge(String edge) {
    this.edge = edge;
  }
}
