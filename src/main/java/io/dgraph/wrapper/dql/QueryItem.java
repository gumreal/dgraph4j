package io.dgraph.wrapper.dql;

import io.dgraph.wrapper.GeneralHelper;
import io.dgraph.wrapper.model.DataType;
import java.io.Serializable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Top Element of a DQL Query */
public class QueryItem implements Serializable {
  private Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

  private String name;
  private boolean reverse;
  private boolean facets;

  private SimpleCondition func;
  private CascadeGroup filter;

  private Set<String> fields;
  private List<QueryItem> edges;

  /** @param name */
  private QueryItem(String name) {
    this(name, false);
  }

  /**
   * @param name
   * @param reverse
   */
  private QueryItem(String name, boolean reverse) {
    this.name = name;
    this.reverse = reverse;
  }

  /**
   * Create a QueryItem with its name
   *
   * @param name the query name
   * @return created object
   */
  public static QueryItem create(String name) {
    return new QueryItem(name);
  }

  /**
   * Create a QueryItem with leading head, specify its reverse property
   *
   * @param name
   * @param reverse this is a reverse edge query or not
   * @return
   */
  public static QueryItem create(String name, boolean reverse) {
    return new QueryItem(name, reverse);
  }

  /**
   * set the main func of this QueryItem
   *
   * @param cond a SimpleCondition
   * @return this QueryItem object
   */
  public QueryItem func(SimpleCondition cond) {
    this.func = cond;
    return this;
  }

  /**
   * filter by a CascadeGroup conditions
   *
   * @param group the CascadeGroup conditions
   * @return this QueryItem object
   */
  public QueryItem filter(CascadeGroup group) {
    this.filter = group;
    return this;
  }

  /**
   * filter by a SimpleGroup conditions
   *
   * @param group the SimpleGroup conditions
   * @return this QueryItem object
   */
  public QueryItem filter(SimpleGroup group) {
    this.filter = new CascadeGroup().withGroup(group);
    return this;
  }

  /**
   * add fields to the result field list, also add uid and dgraph.type
   *
   * @param fieldsToAdd to add
   * @return this QueryItem object
   */
  public QueryItem fields(Collection<String> fieldsToAdd) {
    return fields(fieldsToAdd, true);
  }

  /**
   * add fields to the result field list
   *
   * @param fieldsToAdd to add
   * @param schemaType whether add uid and draph.type to the result list or not
   * @return this QueryItem object
   */
  public QueryItem fields(Collection<String> fieldsToAdd, boolean schemaType) {
    if (null == this.fields) {
      this.fields = new HashSet<>();
    }
    if (schemaType) {
      fieldUid();
      fieldDgraphType();
    }

    this.fields.addAll(fieldsToAdd);
    return this;
  }

  /**
   * add uid to the result field list
   *
   * @return this QueryItem object
   */
  public QueryItem fieldUid() {
    return field(DataType.DT_UID.toString());
  }

  /**
   * add dgraph.type to the result field list
   *
   * @return this QueryItem object
   */
  public QueryItem fieldDgraphType() {
    return field(DataType.DT_DGRAPH_TYPE.toString());
  }

  /**
   * add uid and dgraph.type to the result field list
   *
   * @return this QueryItem object
   */
  public QueryItem fieldBase() {
    return fieldUid().fieldDgraphType();
  }

  /**
   * add field to the result field list
   *
   * @param field to add
   * @return this QueryItem object
   */
  public QueryItem field(String field) {
    if (null == this.fields) {
      this.fields = new HashSet<>();
    }
    fields.add(field);
    return this;
  }

  /**
   * follow an edge, without the edge facets
   *
   * @param edgeQuery the query object to add
   * @return this QueryItem object
   */
  public QueryItem follow(QueryItem edgeQuery) {
    return follow(edgeQuery, false);
  }

  /**
   * follow an edge
   *
   * @param edgeQuery the query object to add
   * @param facets need facet values or not
   * @return this QueryItem object
   */
  public QueryItem follow(QueryItem edgeQuery, boolean facets) {
    if (null == this.edges) {
      this.edges = new ArrayList<>();
    }
    edgeQuery.facets = facets;
    edges.add(edgeQuery);
    return this;
  }

  /**
   * follow an edge in the reverse direction, without the edge facets
   *
   * @param edgeQuery the query object to add
   * @return this QueryItem object
   */
  public QueryItem reverse(QueryItem edgeQuery) {
    return reverse(edgeQuery, false);
  }

  /**
   * follow an edge in the reverse direction
   *
   * @param edgeQuery the query object to add
   * @param facets need facet values or not
   * @return this QueryItem object
   */
  public QueryItem reverse(QueryItem edgeQuery, boolean facets) {
    if (null == this.edges) {
      this.edges = new ArrayList<>();
    }
    if (!edgeQuery.reverse) {
      logger.warn("using reverseEdge method to add a QueryItem which is not a reverse query");
    }

    edgeQuery.facets = facets;
    edges.add(edgeQuery);
    return this;
  }

  /**
   * create the DQL query string, then append it to the buffer
   *
   * @param i level, start from 0, how many "\t" to prefix to each line
   * @param buffer the destination string buffer
   */
  public void appendDql(int i, StringBuffer buffer) {
    String p1 = GeneralHelper.getIndentPrefix(i);

    // head, func, filter
    buffer.append(
        String.format(
            "%s%s%s %s %s{\n",
            p1,
            (reverse ? "~" : "") + name,
            null == func ? "" : "(" + func.toDql(true) + ")",
            (null == filter || !filter.hasExpression()) ? "" : "@filter(" + filter.toDql() + ")",
            facets ? "@facets" : ""));

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
