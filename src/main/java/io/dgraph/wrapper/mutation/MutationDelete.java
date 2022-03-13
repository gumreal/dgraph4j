package io.dgraph.wrapper.mutation;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.Helpers;
import io.dgraph.Transaction;
import java.util.Collection;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class MutationDelete {
  private static Logger LOGGER = LoggerFactory.getLogger(MutationDelete.class.getSimpleName());

  /**
   * Delete an edge: uid --edgeType-- toUid
   *
   * @param client
   * @param uid
   * @param edgeType
   * @param toUid
   */
  public static void deleteEdge(DgraphClient client, String uid, String edgeType, String toUid) {
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation.Builder b =
          DgraphProto.Mutation.newBuilder()
              .addDel(
                  DgraphProto.NQuad.newBuilder()
                      .setSubject(uid)
                      .setPredicate(edgeType)
                      .setObjectId(toUid)
                      .build());
      DgraphProto.Mutation mu = b.build();
      DgraphProto.Response res = txn.mutate(mu);
      txn.commit();

    } catch (Exception e) {
      LOGGER.error("EXCEPTION" + e.getMessage(), e);
    } finally {
    }
  }

  /**
   * Delete edges: uid --edgeType-- toUids
   *
   * @param client
   * @param uid
   * @param edgeType
   * @param toUids
   */
  public static void deleteEdges(
      DgraphClient client, String uid, String edgeType, Collection<String> toUids) {
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation.Builder b = DgraphProto.Mutation.newBuilder();
      Iterator<String> iter = toUids.iterator();
      while (iter.hasNext()) {
        String toUid = iter.next();
        b.addDel(
            DgraphProto.NQuad.newBuilder()
                .setSubject(uid)
                .setPredicate(edgeType)
                .setObjectId(toUid)
                .build());
      }

      DgraphProto.Mutation mu = b.build();
      DgraphProto.Response res = txn.mutate(mu);
      txn.commit();

    } catch (Exception e) {
      LOGGER.error("EXCEPTION" + e.getMessage(), e);
    } finally {
    }
  }

  /**
   * Delete all the specified type edge of one vertex
   *
   * @param client
   * @param uid
   * @param edgeType
   */
  public static void deleteEdgePredicate(DgraphClient client, String uid, String edgeType) {
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mu =
          Helpers.deleteEdges(DgraphProto.Mutation.newBuilder().build(), uid, edgeType);
      System.out.println(mu);

      DgraphProto.Response res = txn.mutate(mu);
      txn.commit();

    } catch (Exception e) {
      LOGGER.error("EXCEPTION" + e.getMessage(), e);
    } finally {
    }
  }

  /**
   * Delete all the specified types of edges of one vertex
   *
   * @param client
   * @param uid
   * @param edgeTypes
   */
  public static void deleteEdgePredicates(
      DgraphClient client, String uid, Collection<String> edgeTypes) {
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation.Builder b = DgraphProto.Mutation.newBuilder();
      Iterator<String> iter = edgeTypes.iterator();
      while (iter.hasNext()) {
        String et = iter.next();
        b.addDel(
            DgraphProto.NQuad.newBuilder()
                .setSubject(uid)
                .setPredicate(et)
                .setObjectValue(DgraphProto.Value.newBuilder().setDefaultVal("_STAR_ALL").build())
                .build());
      }
      DgraphProto.Mutation mu = b.build();
      DgraphProto.Response res = txn.mutate(mu);
      txn.commit();

    } catch (Exception e) {
      LOGGER.error("EXCEPTION" + e.getMessage(), e);
    } finally {
    }
  }

  /**
   * @param client
   * @param uid
   */
  public static void deleteVertex(DgraphClient client, String uid) {
    String dql = String.format(DQL_delete_vertex, uid);
    System.out.println(dql);

    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder().setDeleteJson(ByteString.copyFromUtf8(dql)).build();
      DgraphProto.Response res = txn.mutate(mutation);
      LOGGER.debug(res.toString());
      txn.commit();

    } catch (Exception e) {
      LOGGER.error("EXCEPTION" + e.getMessage(), e);
    } finally {
    }
  }

  private static String DQL_delete_vertex = "{\n\t\"uid\":\"%s\"\n}";
}
