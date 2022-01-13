package io.dgraph.wrapper.schema;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.wrapper.model.DataType;
import io.dgraph.wrapper.model.DateTimeIndexType;
import io.dgraph.wrapper.model.StringIndexType;
import java.util.Set;

/** Schema Tool */
public class SchemaHelper {

  /**
   * alter predicate
   *
   * @param client
   * @param predicate
   * @param dt
   */
  public static void alterPredicate(DgraphClient client, String predicate, DataType dt) {
    alterPredicate(client, predicate, dt, false);
  }

  /**
   * alter predicate
   *
   * @param client
   * @param predicate
   * @param dt
   */
  public static void alterPredicate(
      DgraphClient client, String predicate, DataType dt, boolean createIndex) {
    String indexName = "";
    if (createIndex) {
      switch (dt) {
        case DT_STRING:
          indexName = StringIndexType.exact.name();
          break;
        case DT_DATETIME:
          indexName = DateTimeIndexType.day.name();
        default:
          indexName = dt.toString();
      }
      alterPredicate(client, predicate, dt, true, indexName);
    } else {
      alterPredicate(client, predicate, dt, false, indexName);
    }
  }

  /**
   * alter predicate
   *
   * @param client
   * @param predicate
   * @param sit
   */
  public static void alterPredicate(DgraphClient client, String predicate, StringIndexType sit) {
    alterPredicate(client, predicate, DataType.DT_STRING, true, sit.name());
  }

  /**
   * alter predicate
   *
   * @param client
   * @param predicate
   * @param dtit
   */
  public static void alterPredicate(DgraphClient client, String predicate, DateTimeIndexType dtit) {
    alterPredicate(client, predicate, DataType.DT_DATETIME, true, dtit.name());
  }

  /**
   * alter predicate
   *
   * @param predicate
   * @param dt
   * @param createIndex
   * @param indexName
   * @return
   */
  protected static void alterPredicate(
      DgraphClient client, String predicate, DataType dt, boolean createIndex, String indexName) {
    String schema = String.format("%s: %s", predicate, dt.toString());
    if (createIndex) {
      schema += String.format(" @index(%s)", indexName);
    }
    schema += " .";

    DgraphProto.Operation op = DgraphProto.Operation.newBuilder().setSchema(schema).build();
    client.alter(op);
  }

  /**
   * alter dgraph.type
   *
   * @param client
   * @param typeName
   * @param predicates
   */
  public static void alterType(DgraphClient client, String typeName, Set<String> predicates) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("type " + typeName + " {\n");
    if (null != predicates) {
      predicates.forEach(s -> buffer.append("   " + s + "\n"));
    }
    buffer.append("}");
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder().setSchema(buffer.toString()).build();
    client.alter(op);
  }

  /**
   * @param client
   * @param edgeType
   * @param needReverse
   */
  public static void alterEdge(DgraphClient client, String edgeType, boolean needReverse) {
    String schema = String.format("%s uid %s .", edgeType, needReverse ? "@reverse" : "");
    DgraphProto.Operation op = DgraphProto.Operation.newBuilder().setSchema(schema).build();
    client.alter(op);
  }

  /**
   * set raw dgraph schema
   *
   * @param client
   * @param schema
   */
  public static void alter(DgraphClient client, String schema) {
    DgraphProto.Operation op = DgraphProto.Operation.newBuilder().setSchema(schema).build();
    client.alter(op);
  }
}
