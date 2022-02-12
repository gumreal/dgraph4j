package io.dgraph.wrapper.query;

import com.google.gson.*;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.wrapper.GeneralHelper;
import io.dgraph.wrapper.model.VertexBase;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class QueryHelper {
  private static Logger LOGGER = LoggerFactory.getLogger(QueryHelper.class.getSimpleName());

  /**
   * @param client
   * @param dql
   * @return
   */
  public static String dqlQuery(DgraphClient client, String dql) {
    DgraphProto.Response res = client.newTransaction().query(dql);
    return res.getJson().toStringUtf8();
  }

  /**
   * Get vertx, with its edge to Vertx
   *
   * @param obj
   * @return
   */
  public static VertexBase getVertexByUid(
      DgraphClient client, VertexBase obj, Collection<CascadeEdge> filters) {
    // check
    if (null == client) {
      LOGGER.warn("invalid dgraph client");
      return null;
    }
    if (null == obj || null == obj.getUid()) {
      LOGGER.warn("invalid vertx");
      return null;
    }
    int validCascadeCount = 0;
    if (null != filters && filters.size() > 0) {
      Iterator<CascadeEdge> iter = filters.iterator();
      while (iter.hasNext()) {
        CascadeEdge f = iter.next();
        if (GeneralHelper.isEmpty(f.getEdgeType()) || null == f.getOtherVertx()) {
          LOGGER.warn("invalid CascadeEdge");
          return null;
        } else {
          validCascadeCount++;
        }
      }
    }

    // dql
    String dql =
        String.format(
            DQL_vertex_get,
            obj.getClass().getSimpleName(),
            obj.getUid(),
            makeQueryPredicateStr(obj.getPredicates(), false, 2),
            validCascadeCount > 0 ? makeEdgeVertxQueryPredicates(filters, 2) : "");
    LOGGER.debug(dql);

    // query
    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);

    // parse result
    JsonObject jo = new Gson().fromJson(resultStr, JsonObject.class);
    JsonElement je = jo.get(obj.getClass().getSimpleName());
    if (null == je) {
      LOGGER.warn("invalid response:" + resultStr);
      return null;
    }
    JsonArray ja = je.getAsJsonArray();
    if (null == ja || ja.size() == 0) {
      String log = String.format("[%s]%s: not found", obj.getClass().getSimpleName(), obj.getUid());
      LOGGER.debug(log);
      return null;
    }
    String jsonStr = ja.get(0).getAsJsonObject().toString();

    // done
    return obj.mergeJson(jsonStr);
  }

  /**
   * @param predicates
   * @param withUid
   * @param indentLevel
   * @return
   */
  protected static String makeQueryPredicateStr(
      Set<String> predicates, boolean withUid, int indentLevel) {
    String linePrefix = GeneralHelper.getIndentPrefix(indentLevel);
    StringBuffer buffer = new StringBuffer();
    if (withUid) {
      buffer.append(linePrefix + "uid\n");
    }
    predicates.forEach(s -> buffer.append(linePrefix + s + "\n"));
    return buffer.toString();
  }

  /**
   * @param filters
   * @param indentLevel
   * @return
   */
  protected static String makeEdgeVertxQueryPredicates(
      Collection<CascadeEdge> filters, int indentLevel) {
    if (null == filters || filters.size() == 0) {
      return "";
    }

    StringBuffer buffer = new StringBuffer();
    String prefixObj = GeneralHelper.getIndentPrefix(indentLevel);
    filters.forEach(
        f -> {
          if (null == f || GeneralHelper.isEmpty(f.getEdgeType()) || null == f.getOtherVertx()) {
            return;
          }

          buffer.append(prefixObj + (f.isReverse() ? "~" : "") + f.getEdgeType() + "{\n");
          buffer.append(
              makeQueryPredicateStr(f.getOtherVertx().getPredicates(), true, indentLevel + 1));
          buffer.append(prefixObj + "}\n");
        });
    return buffer.toString();
  }

  private static String DQL_vertex_get =
      "{\n" + "   %s(func:uid(%s)){\n" + "%s\n" + "%s\n" + "   }\n" + "}";

  /**
   * @param client
   * @param predicate
   * @param v
   * @return
   */
  public static List<String> getUidByPredicate(DgraphClient client, String predicate, String v) {
    if (null == client || GeneralHelper.isEmpty(predicate) || GeneralHelper.isEmpty(v)) {
      LOGGER.warn("invalid input");
      return null;
    }

    String dql = String.format(DQL_uid_get_by_predicate, predicate, predicate, v, predicate);
    LOGGER.debug(dql);

    // query
    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);

    // parse result
    return makeUidList(resultStr, predicate);
  }

  private static String DQL_uid_get_by_predicate =
      "{\n" + "\t%s(func:eq(%s, \"%s\")){\n" + "\t\tuid\n" + "\t\t%s\n" + "\t}\n" + "}";

  /**
   * @param resultStr
   * @param predicate
   * @return
   */
  protected static List<String> makeUidList(String resultStr, String predicate) {
    JsonObject jo = new Gson().fromJson(resultStr, JsonObject.class);
    if (null == jo || !jo.has(predicate)) {
      return null;
    }

    List<String> resultArr = new ArrayList<>();
    JsonArray ja = jo.get(predicate).getAsJsonArray();
    if (null == ja || ja.size() == 0) {
      return resultArr;
    }
    for (int i = 0; i < ja.size(); i++) {
      resultArr.add(ja.get(i).getAsJsonObject().get("uid").getAsString());
    }
    return resultArr;
  }

  /**
   * @param client
   * @param conditions
   * @return
   */
  public static List<String> getUidByPredicates(
      DgraphClient client, Map<String, Object> conditions) {
    if (null == client || null == conditions || conditions.size() == 0) {
      LOGGER.warn("invalid input");
      return null;
    }

    String predicate1 = null;
    Object value1 = null;
    StringBuffer otherConditions = new StringBuffer();
    StringBuffer predicates = new StringBuffer();
    predicates.append("\t\tuid\n");
    int i = 0;
    for (String k : conditions.keySet()) {
      if (0 == i) {
        predicate1 = k;
        value1 = conditions.get(k);
      } else {
        if (1 == i) {
          otherConditions.append(String.format("@filter(eq(%s, %s)", k, conditions.get(k)));
        } else {
          otherConditions.append(String.format(" AND eq(%s, %s)", k, conditions.get(k)));
        }
      }

      predicates.append(String.format("\t\t%s\n", k));
      i++;
    }
    if (otherConditions.length() > 0) {
      otherConditions.append(")");
    }
    String dql =
        String.format(
            DQL_uid_get_by_predicates,
            predicate1,
            value1,
            otherConditions.toString(),
            predicates.toString());
    LOGGER.debug(dql);

    // query
    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);

    // parse result
    return makeUidList(resultStr, "result");
  }

  // @filter(eq(ext, "ext01") AND eq(e1, "e1_v"))
  //  bundle
  //  ext
  //  e1
  private static String DQL_uid_get_by_predicates =
      "{\n" + "\tresult(func: eq(%s, \"%s\")) %s{\n" + "%s\n" + "\t}\n" + "}";

  /**
   * [ref SQL] <br>
   * SELECT vertx, count(edge) AS count_edge FROM edge GROUP BY vertx
   *
   * @param client
   * @param vertx
   * @param values
   * @param edge
   * @return
   */
  public static Map<String, Long> vertexEdgeCount(
      DgraphClient client, String vertx, Set<String> values, String edge) {
    // check
    if (!checkInput(client, vertx, values, edge)) {
      return null;
    }

    // query
    String dql =
        String.format(
            DQL_GroupBy_Count, vertx, GeneralHelper.implode(values, ",", "\"", "\""), vertx, edge);
    LOGGER.debug(dql);

    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);

    // parse result
    Map<String, Long> resultMap = new HashMap<>();
    JsonObject jo = new Gson().fromJson(resultStr, JsonObject.class);
    JsonArray jsonArray = jo.getAsJsonArray("result");
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonObject object = jsonArray.get(i).getAsJsonObject();
      String k = object.get(vertx).getAsString();
      long v = object.has("count") ? object.get("count").getAsLong() : 0l;
      resultMap.put(k, resultMap.containsKey(k) ? (resultMap.get(k) + v) : v);
    }
    return resultMap;
  }

  private static String DQL_GroupBy_Count =
      "{\n"
          + "   result(func:eq(%s, %s)){\n"
          + "       %s\n"
          + "       count: count(%s)\n"
          + "   }\n"
          + "}";

  /**
   * [ref SQL] <br>
   * SELECT sum(count_edge) FROM <br>
   * SELECT vertx, count(edge) AS count_edge FROM edge GROUP BY vertx
   *
   * @param client
   * @param vertx
   * @param values
   * @param edge
   * @return
   */
  public static Long vertxEdgeCountSum(
      DgraphClient client, String vertx, Set<String> values, String edge) {
    // check
    if (!checkInput(client, vertx, values, edge)) {
      return null;
    }

    // query
    String dql =
        String.format(
            DQL_GroupBy_Count_Sum,
            vertx,
            GeneralHelper.implode(values, ",", "\"", "\""),
            edge,
            edge,
            edge);
    LOGGER.debug(dql);
    System.out.println(dql);

    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);
    System.out.println(resultStr);

    // parse result
    JsonArray arr = parseResultArr(resultStr);
    if (null == arr || arr.size() == 0) {
      return null;
    }
    JsonElement je = arr.get(0).getAsJsonObject().get("sum");
    if (null == je || je instanceof JsonNull) {
      return 0l;
    } else {
      return arr.get(0).getAsJsonObject().get("sum").getAsLong();
    }
  }

  private static String DQL_GroupBy_Count_Sum =
      "{\n"
          + "   var(func:eq(%s, %s)){\n"
          + "       count_%s as count(%s)\n"
          + "   }\n"
          + "   result(){\n"
          + "       sum: sum(val(count_%s))\n"
          + "   }\n"
          + "}";

  /**
   * @param client
   * @param node
   * @param values
   * @param edge
   * @return
   */
  private static boolean checkInput(
      DgraphClient client, String node, Set<String> values, String edge) {
    if (null == client) {
      LOGGER.warn("invalid dgraph client");
      return false;
    }
    if (GeneralHelper.isEmpty(node)) {
      LOGGER.warn("null node");
      return false;
    }
    if (null == values || values.size() == 0) {
      LOGGER.warn("invalid node values");
      return false;
    }
    if (GeneralHelper.isEmpty(edge)) {
      LOGGER.warn("invalid edge");
      return false;
    }
    return true;
  }

  /**
   * @param client
   * @param filters
   * @return
   */
  protected static Long intersectCount(DgraphClient client, List<VertxEdgeFilter> filters) {
    return interOrUnionCount(client, filters, "AND");
  }

  /**
   * @param client
   * @param filters
   * @return
   */
  protected static Long unionCount(DgraphClient client, List<VertxEdgeFilter> filters) {
    return interOrUnionCount(client, filters, "OR");
  }

  /**
   * @param client
   * @param filters
   * @return
   */
  protected static Long interOrUnionCount(
      DgraphClient client, List<VertxEdgeFilter> filters, String op) {
    // check
    if (null == client) {
      LOGGER.warn("invalid dgraph client");
      return null;
    }
    if (null == filters || filters.size() == 0) {
      LOGGER.warn("invalid filters");
      return null;
    }

    // query
    StringBuffer filterBuffer = new StringBuffer();
    int i = 0;
    List<String> varList = new ArrayList<>();
    Iterator<VertxEdgeFilter> iter = filters.iterator();
    while (iter.hasNext()) {
      VertxEdgeFilter vef = iter.next();
      if (!vef.isValid()) {
        LOGGER.warn("invalid filter: " + vef);
        return null;
      }
      String var = "var" + (i++);
      varList.add(var);
      filterBuffer.append(vef.getQueryPhrase(var));
    }

    String varStr = GeneralHelper.implode(varList, ",");
    String varUidStr = GeneralHelper.implode(varList, " " + op + " ", "uid(", ")");
    String dql =
        String.format(DQL_Intersect_Union_Count, filterBuffer.toString(), varStr, varUidStr);
    LOGGER.debug(dql);

    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);

    // parse result
    JsonArray arr = parseResultArr(resultStr);
    if (null == arr || arr.size() == 0) {
      return null;
    }
    return arr.get(0).getAsJsonObject().get("count").getAsLong();
  }

  private static String DQL_Intersect_Union_Count =
      "{\n"
          + "  %s\n"
          + "  result(func:uid(%s)) @filter(%s){\n"
          + "    count:count(uid)\n"
          + "  }\n"
          + "}";

  /**
   * @param jsonStr
   * @return
   */
  private static JsonArray parseResultArr(String jsonStr) {
    try {
      JsonObject jo = new Gson().fromJson(jsonStr, JsonObject.class);
      return jo.getAsJsonArray("result");

    } catch (Exception e) {
      return null;
    }
  }

  /**
   * @param client
   * @param uid
   * @param prediction
   * @param facet
   * @return
   */
  public static int sumFacet(DgraphClient client, String uid, String prediction, String facet) {
    // check
    if (null == client
        || GeneralHelper.isEmpty(uid)
        || GeneralHelper.isEmpty(prediction)
        || GeneralHelper.isEmpty(facet)) {
      LOGGER.warn("invalid parameter");
      return 0;
    }

    // query
    String dql = String.format(DQL_sum_facet, uid, prediction, facet);
    LOGGER.debug(dql);

    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);

    // parse result
    JsonArray arr = parseResultArr(resultStr);
    if (null == arr || arr.size() == 0) {
      LOGGER.warn("invalid result");
      return 0;
    }
    return arr.get(0).getAsJsonObject().get("sum(val(item))").getAsInt();
  }

  private static String DQL_sum_facet =
      "{\n"
          + "\tresult(func:uid(%s)){\n"
          + "\t\t%s @facets(item as %s)\n"
          + "\t\tsum(val(item))\n"
          + "\t}\n"
          + "}";
}
