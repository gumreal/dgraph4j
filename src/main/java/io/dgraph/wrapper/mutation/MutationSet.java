package io.dgraph.wrapper.mutation;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;

/** */
public class MutationSet {
  /**
   * @param client
   * @param json
   */
  public static void set(DgraphClient client, String json) {
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json)).build();
      txn.mutate(mutation);
      txn.commit();
    } finally {
      txn.discard();
    }
  }
}
