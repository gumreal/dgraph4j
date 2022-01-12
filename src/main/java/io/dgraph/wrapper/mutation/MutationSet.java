package io.dgraph.wrapper.mutation;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.dgraph.wrapper.GeneralHelper;
import io.dgraph.wrapper.model.DgraphTypeBase;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class MutationSet {
  private static Logger LOGGER = LoggerFactory.getLogger(MutationSet.class.getSimpleName());

  /**
   * send a Dgraph Type object to Set Command
   *
   * @param client
   * @param obj
   */
  public static String setVertx(DgraphClient client, DgraphTypeBase obj) {
    if (null == obj) {
      return null;
    }

    String uid = obj.getUid();
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder()
              .setSetJson(ByteString.copyFromUtf8(obj.toJson()))
              .build();
      DgraphProto.Response res = txn.mutate(mutation);
      LOGGER.debug(res.toString());

      Map<String, String> uidsMap = res.getUidsMap();
      if (null != uidsMap && uidsMap.size() == 1) {
        for (String s : uidsMap.values()) {
          uid = s;
          break;
        }
      }

      txn.commit();

    } finally {
      txn.discard();
    }

    // done
    return uid;
  }

  /**
   * send list of Dgraph Type objects to Set Command
   *
   * @param client
   * @param list
   */
  public static Collection<DgraphTypeBase> setVertx(
      DgraphClient client, Collection<DgraphTypeBase> list) {
    if (null == list || list.size() == 0) {
      return null;
    }

    // set stub variable for uid
    int i = 0;
    Map<String, DgraphTypeBase> varUidMap = new HashMap<>();
    Iterator<DgraphTypeBase> iter = list.iterator();
    while (iter.hasNext()) {
      DgraphTypeBase dt = iter.next();
      if (null == dt.getUid()) {
        String varUid = "var" + (i++);
        dt.setUid("_:" + varUid);
        varUidMap.put(varUid, dt);
      }
    }

    // set to dgraph
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder()
              .setSetJson(ByteString.copyFromUtf8(new Gson().toJson(list)))
              .build();
      DgraphProto.Response res = txn.mutate(mutation);
      LOGGER.debug(res.toString());

      // parse result
      Map<String, String> uidsMap = res.getUidsMap();
      if (null != uidsMap) {
        uidsMap
            .keySet()
            .forEach(
                s -> {
                  if (varUidMap.containsKey(s)) {
                    varUidMap.get(s).setUid(uidsMap.get(s));
                  }
                });
      }

      txn.commit();

    } finally {
      txn.discard();
    }

    // done
    return list;
  }

  /**
   * Create Edge: fromUid --edgeType--> toUid
   *
   * @param client
   * @param fromUid
   * @param edgeType
   * @param toUid
   * @return
   */
  public static boolean setEdge(
      DgraphClient client, String fromUid, String edgeType, String toUid) {
    List<String> list = new ArrayList<>();
    list.add(toUid);
    return setEdges(client, fromUid, edgeType, list);
  }

  /**
   * Create Edges: fromUid --edgeType--> toUids
   *
   * @param client
   * @param fromUid
   * @param edgeType
   * @param toUids
   * @return
   */
  public static boolean setEdges(
      DgraphClient client, String fromUid, String edgeType, Collection<String> toUids) {
    if (GeneralHelper.isEmpty(fromUid)
        || GeneralHelper.isEmpty(edgeType)
        || null == toUids
        || toUids.size() == 0) {
      return false;
    }

    StringBuffer buffer = new StringBuffer();
    for (String to : toUids) {
      buffer.append(String.format(DQL_triple, fromUid, edgeType, to));
    }
    String dql = String.format(DQL_set_edge, buffer);

    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(dql)).build();
      DgraphProto.Response res = txn.mutate(mutation);
      LOGGER.debug(res.toString());

      txn.commit();

    } catch (Exception e) {

    } finally {
      txn.discard();
    }

    // done
    return true;
  }

  private static final String DQL_set_edge = "{\n" + "   set{\n" + "       %s\n" + "   }\n" + "}";
  private static final String DQL_triple = "<%s> <%s> <%s>\n";

  /**
   * send raw json for Set Command
   *
   * @param client
   * @param json
   */
  public static Collection<String> setJson(DgraphClient client, String json) {
    Collection<String> result = null;
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json)).build();
      DgraphProto.Response res = txn.mutate(mutation);
      LOGGER.debug(res.toString());

      Map<String, String> uidsMap = res.getUidsMap();
      if (null != uidsMap) {
        result = uidsMap.values();
      }

      txn.commit();

    } finally {
      txn.discard();
    }

    return result;
  }
}
