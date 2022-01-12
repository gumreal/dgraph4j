package io.dgraph.wrapper.mutation;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import io.dgraph.wrapper.schema.DgraphTypeBase;

import java.util.List;

/** */
public class MutationSet {

  /**
   * send a Dgraph Type object to Set Command
   *
   * @param client
   * @param obj
   */
  public static void set(DgraphClient client, DgraphTypeBase obj){
    if(null == obj){
      return;
    }
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation = DgraphProto.Mutation.newBuilder()
              .setSetJson(ByteString.copyFromUtf8(obj.toJson()))
              .build();
      DgraphProto.Response res = txn.mutate(mutation);
      System.out.println(res.toString());

      txn.commit();

    } finally {
      txn.discard();
    }
  }

  /**
   * send list of Dgraph Type objects to Set Command
   *
   * @param client
   * @param list
   */
  public static void set(DgraphClient client, List<DgraphTypeBase> list){
    if(null == list || list.size()==0){
      return;
    }

    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation = DgraphProto.Mutation.newBuilder()
              .setSetJson(ByteString.copyFromUtf8(new Gson().toJson(list)))
              .build();
      DgraphProto.Response res = txn.mutate(mutation);
      System.out.println(res.toString());

      txn.commit();

    } finally {
      txn.discard();
    }
  }

  /**
   * send raw json for Set Command
   *
   * @param client
   * @param json
   */
  public static void set(DgraphClient client, String json) {
    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation mutation =
          DgraphProto.Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json)).build();
      DgraphProto.Response res = txn.mutate(mutation);
      System.out.println(res.toString());

      txn.commit();
    } finally {
      txn.discard();
    }
  }
}
