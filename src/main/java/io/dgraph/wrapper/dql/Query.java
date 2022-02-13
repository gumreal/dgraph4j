package io.dgraph.wrapper.dql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Query implements Serializable {
  List<QueryItem> items = new ArrayList<>();

  public Query addItem(QueryItem item) {
    items.add(item);
    return this;
  }

  /**
   * TODO generate GQL
   *
   * @return
   */
  public String toDql() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("{\n");
    items.forEach(
        item -> {
          item.appendDql(1, buffer);
          buffer.append("\t\n");
        });
    buffer.append("}");
    return buffer.toString();
  }
}
