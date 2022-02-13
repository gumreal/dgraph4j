package io.dgraph.wrapper.dql;

import io.dgraph.wrapper.GeneralHelper;
import io.dgraph.wrapper.model.DataType;
import java.io.Serializable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryItem implements Serializable {
  private Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

  private boolean reverse;
  private String head;
  private SimpleCondition func;
  private CascadeGroup filter;

  private Set<String> fields;
  private List<QueryItem> edges;

  /** @param head */
  private QueryItem(String head) {
    this(head, false);
  }

  /**
   * @param head
   * @param reverse
   */
  private QueryItem(String head, boolean reverse) {
    this.head = head;
    this.reverse = reverse;
  }

  /**
   * @param head
   * @return
   */
  public static QueryItem create(String head) {
    return new QueryItem(head);
  }

  /**
   * @param head
   * @param reverse
   * @return
   */
  public static QueryItem create(String head, boolean reverse) {
    return new QueryItem(head, reverse);
  }

  /**
   * @param cond
   * @return
   */
  public QueryItem func(SimpleCondition cond) {
    this.func = cond;
    return this;
  }

  /**
   * @param group
   * @return
   */
  public QueryItem filter(CascadeGroup group) {
    this.filter = group;
    return this;
  }

  /**
   * @param group
   * @return
   */
  public QueryItem filter(SimpleGroup group) {
    this.filter = new CascadeGroup().withGroup(group);
    return this;
  }

  /**
   * @param fieldsToAdd
   * @return
   */
  public QueryItem fields(Collection<String> fieldsToAdd) {
    return fields(fieldsToAdd, true);
  }

  /**
   * @param fieldsToAdd
   * @param schemaType
   * @return
   */
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

  /**
   * @param edgeQuery
   * @return
   */
  public QueryItem follow(QueryItem edgeQuery) {
    if (null == this.edges) {
      this.edges = new ArrayList<>();
    }
    edges.add(edgeQuery);
    return this;
  }

  /**
   * @param edgeQuery
   * @return
   */
  public QueryItem reverse(QueryItem edgeQuery) {
    if (null == this.edges) {
      this.edges = new ArrayList<>();
    }
    if (!edgeQuery.reverse) {
      logger.warn("using reverseEdge method to add a QueryItem which is not a reverse query");
    }

    edges.add(edgeQuery);
    return this;
  }

  /**
   * @param i
   * @param buffer
   */
  public void appendDql(int i, StringBuffer buffer) {
    String p1 = GeneralHelper.getIndentPrefix(i);

    // head, func, filter
    buffer.append(
        String.format(
            "%s%s%s %s{\n",
            p1,
            (reverse ? "~" : "") + head,
            null == func ? "" : "(" + func.toDql(true) + ")",
            (null == filter || !filter.hasExpression()) ? "" : "@filter(" + filter.toDql() + ")"));

    // fields
    String p2 = GeneralHelper.getIndentPrefix(i + 1);
    if (null != fields) {
      fields.forEach(s -> buffer.append(p2 + s + "\n"));
    }

    // edges
    if (null != edges) {
      buffer.append(p2 + "\n");
      edges.forEach(item -> item.appendDql(i + 1, buffer));
    }

    buffer.append(p1 + "}\n");
  }
}
