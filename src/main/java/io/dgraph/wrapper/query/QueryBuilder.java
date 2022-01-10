package io.dgraph.wrapper.query;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.wrapper.GeneralHelper;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class QueryBuilder {
  private static Logger LOGGER = LoggerFactory.getLogger(QueryBuilder.class.getSimpleName());

  /**
   * [ref SQL] SELECT node, count(edge) AS count_edge FROM edge GROUP BY node
   *
   * @param client
   * @param node
   * @param values
   * @param edge
   * @return
   */
  public static String nodeEdgeCount(
      DgraphClient client, String node, Set<String> values, String edge) {
    if (null == client || values.size() == 0) {
      LOGGER.warn("invalid dgraph client");
      return null;
    }
    if (GeneralHelper.isEmpty(node)) {
      LOGGER.warn("null node");
      return null;
    }
    if (null == values || values.size() == 0) {
      LOGGER.warn("invalid node values");
      return null;
    }
    if (GeneralHelper.isEmpty(edge)) {
      LOGGER.warn("invalid edge");
      return null;
    }

    String dql =
        String.format(DQL_GroupBy_Count, node, GeneralHelper.implode(values, ","), node, edge);
    LOGGER.debug(dql);

    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);
    return resultStr;
  }

  private static String DQL_GroupBy_Count =
      "{\n"
          + "   result(func:eq(%s, %s)){\n"
          + "       %s\n"
          + "       count: count(%s)\n"
          + "   }\n"
          + "}";

  /**
   * [ref SQL] SELECT sum(count_edge) FROM SELECT node, count(edge) AS count_edge FROM edge GROUP BY
   * node
   *
   * @param client
   * @param node
   * @param values
   * @param edge
   * @return
   */
  public static String nodeEdgeCountSum(
      DgraphClient client, String node, Set<String> values, String edge) {
    if (null == client || values.size() == 0) {
      LOGGER.warn("invalid dgraph client");
      return null;
    }
    if (GeneralHelper.isEmpty(node)) {
      LOGGER.warn("null node");
      return null;
    }
    if (null == values || values.size() == 0) {
      LOGGER.warn("invalid node values");
      return null;
    }
    if (GeneralHelper.isEmpty(edge)) {
      LOGGER.warn("invalid edge");
      return null;
    }

    String dql =
        String.format(
            DQL_GroupBy_Count_Sum, node, GeneralHelper.implode(values, ","), edge, edge, edge);
    LOGGER.debug(dql);

    DgraphProto.Response res = client.newTransaction().query(dql);
    String resultStr = res.getJson().toStringUtf8();
    LOGGER.debug(resultStr);
    return resultStr;
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
}
