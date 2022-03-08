package io.dgraph.wrapper.dql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** a DQL query */
public class Query implements Serializable {
  List<QueryItem> items = new ArrayList<>();

  /**
   * add a sub query
   *
   * @param item to add
   * @return this Query object
   */
  public Query addItem(QueryItem item) {
    if (null != item) {
      items.add(item);
    }
    return this;
  }

  /**
   * add queries
   *
   * @param toAddItems
   * @return
   */
  public Query addItems(List<QueryItem> toAddItems) {
    if (null != toAddItems) {
      items.addAll(toAddItems);
    }
    return this;
  }

  /**
   * generate DQL
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
