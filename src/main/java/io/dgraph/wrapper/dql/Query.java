package io.dgraph.wrapper.dql;

import java.util.ArrayList;
import java.util.List;

public class Query {
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
    return "";
  }
}
