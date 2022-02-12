package io.dgraph.wrapper.dql;

import io.dgraph.wrapper.model.DataType;
import java.util.*;

public class QueryItem {
  private String head;
  private SimpleCondition func;
  private CascadeGroup filter;

  private Set<String> fields;
  private List<QueryItem> edges;

  private QueryItem(String head) {
    this.head = head;
  }

  public static QueryItem create(String head) {
    return new QueryItem(head);
  }

  public QueryItem func(SimpleCondition cond) {
    this.func = cond;
    return this;
  }

  public QueryItem filter(CascadeGroup group) {
    this.filter = group;
    return this;
  }

  public QueryItem filter(SimpleGroup group) {
    this.filter = new CascadeGroup().withGroup(group);
    return this;
  }

  public QueryItem fields(Collection<String> fieldsToAdd) {
    return fields(fieldsToAdd, true);
  }

  public QueryItem fields(Collection<String> fieldsToAdd, boolean schemaType) {
    if (null == this.fields) {
      this.fields = new HashSet<>();
    }
    if (schemaType) {
      this.fields.add(DataType.DT_UID.toString());
      this.fields.add(DataType.DT_DGRAPH_TYPE.toString());
    }

    this.fields.addAll(fieldsToAdd);
    return this;
  }

  public QueryItem field(String field) {
    if (null == this.fields) {
      this.fields = new HashSet<>();
    }
    fields.add(field);
    return this;
  }

  public QueryItem followEdge(QueryItem edgeQuery) {
    if (null == this.edges) {
      this.edges = new ArrayList<>();
    }
    edges.add(edgeQuery);
    return this;
  }
}
