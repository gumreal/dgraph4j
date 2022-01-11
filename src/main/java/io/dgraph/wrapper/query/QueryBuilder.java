package io.dgraph.wrapper.query;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.wrapper.GeneralHelper;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class QueryBuilder {
  private static Logger LOGGER = LoggerFactory.getLogger(QueryBuilder.class.getSimpleName());

  /**
   * [ref SQL] <br>
   * SELECT node, count(edge) AS count_edge FROM edge GROUP BY node
   *
   * @param client
   * @param node
   * @param values
   * @param edge
   * @return
   */
  public static Map<String, Long> nodeEdgeCount(
      DgraphClient client, String node, Set<String> values, String edge) {
    // check
    if (!checkInput(client, node, values, edge)) {
      return null;
    }

    // query
    String dql =
        String.format(DQL_GroupBy_Count, node, GeneralHelper.implode(values, ","), node, edge);
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
      resultMap.put(object.get(node).getAsString(), object.get("count").getAsLong());
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
   * SELECT node, count(edge) AS count_edge FROM edge GROUP BY node
   *
   * @param client
   * @param node
   * @param values
   * @param edge
   * @return
   */
  public static Long nodeEdgeCountSum(
      DgraphClient client, String node, Set<String> values, String edge) {
    // check
    if (!checkInput(client, node, values, edge)) {
      return null;
    }

    // query
    String dql =
        String.format(
            DQL_GroupBy_Count_Sum, node, GeneralHelper.implode(values, ","), edge, edge, edge);
    LOGGER.debug(dql);
    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);

    // parse result
    JsonArray arr = parseResultArr(resultStr);
    if (null == arr || arr.size() == 0) {
      return null;
    }
    return arr.get(0).getAsJsonObject().get("sum").getAsLong();
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
    if (null == client || values.size() == 0) {
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
}
