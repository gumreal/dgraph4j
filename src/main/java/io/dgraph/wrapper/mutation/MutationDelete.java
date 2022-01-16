package io.dgraph.wrapper.mutation;

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
   * Delete an edge: uid --edgeType--> toUid
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
      LOGGER.debug(res.toString());

    } finally {
      txn.discard();
    }
  }

  /**
   * Delete edges: uid --edgeType--> toUids
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
      LOGGER.debug(res.toString());

    } finally {
      txn.discard();
    }
  }

  /**
   * Delete all the specified type edges of one vertex
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
      LOGGER.debug(res.toString());

    } finally {
      txn.discard();
    }
  }

  /**
   * @param client
   * @param uid
   */
  public void deleteVertex(DgraphClient client, String uid) {
    // TODO
  }
}
