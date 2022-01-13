package io.dgraph.wrapper.mutation;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.dgraph.wrapper.GeneralHelper;
import io.dgraph.wrapper.model.SimpleEdge;
import io.dgraph.wrapper.model.VertxBase;
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
  public static String setVertx(DgraphClient client, VertxBase obj) {
    if (null == obj) {
      return null;
    }

    String uid = obj.getUid();
    String requestJson = obj.toJson();
    LOGGER.debug(requestJson);

    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder()
              .setSetJson(ByteString.copyFromUtf8(requestJson))
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
  public static Collection<VertxBase> setVertx(DgraphClient client, Collection<VertxBase> list) {
    if (null == list || list.size() == 0) {
      return null;
    }

    // set stub variable for uid
    int i = 0;
    Map<String, VertxBase> varUidMap = new HashMap<>();
    Iterator<VertxBase> iter = list.iterator();
    while (iter.hasNext()) {
      VertxBase dt = iter.next();
      if (null == dt.getUid()) {
        String varUid = "var" + (i++);
        dt.setUid("_:" + varUid);
        varUidMap.put(varUid, dt);
      }
    }

    String requestJson = new Gson().toJson(list);
    LOGGER.debug(requestJson);

    // set to dgraph
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder()
              .setSetJson(ByteString.copyFromUtf8(requestJson))
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
    buffer.append("[\n");
    int i = 0;
    for (String to : toUids) {
      if (i++ > 0) {
        buffer.append(",\n");
      }
      buffer.append(new SimpleEdge(fromUid, edgeType, to).toLinkJson());
    }
    buffer.append("]\n");
    String dql = String.format(DQL_set_edge, buffer);
    LOGGER.debug(dql);

    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(dql)).build();
      DgraphProto.Response res = txn.mutate(mutation);
      LOGGER.debug(res.toString());

      txn.commit();

    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      return false;

    } finally {
      txn.discard();
    }

    // done
    return true;
  }

  private static final String DQL_set_edge = "{\n" + "   \"set\":%s\n" + "}";

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
