package io.dgraph.wrapper.mutation;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class MutationDelete {
  private static Logger LOGGER = LoggerFactory.getLogger(MutationDelete.class.getSimpleName());

  /**
   * { delete { <0x4e6f> <release_in> <0x4e70> . } }
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
                      .setObjectValue(DgraphProto.Value.newBuilder().setDefaultVal(toUid).build())
                      .build());
      DgraphProto.Response res = txn.mutate(b.build());
      LOGGER.debug(res.toString());
      System.out.println(res);
      txn.commit();

    } finally {
      txn.discard();
    }
  }

  private static String DQL_delete_one_edge =
      "{\n"
          + "\t\"delete\" : {\n"
          + "\t\t\"uid\": \"%s\",\n"
          + "\t\t\"%s\":[\n"
          + "\t\t\t{\n"
          + "\t\t\t\t\"uid\": \"%s\"\n"
          + "\t\t\t}\t\n"
          + "\t\t]\n"
          + "\t}\n"
          + "}";

  /**
   * @param client
   * @param uid
   * @param edgeType
   * @param toUids
   */
  public void deleteEdges(
      DgraphClient client, String uid, String edgeType, Collection<String> toUids) {
    // TODO
  }

  /**
   * @param client
   * @param uid
   * @param edgeType
   */
  public void deleteEdges(DgraphClient client, String uid, String edgeType) {
    // TODO
  }

  /**
   * @param client
   * @param uid
   */
  public void deleteVertex(DgraphClient client, String uid) {
    // TODO
  }
}
