package io.dgraph.wrapper.mutation;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mutation {
  private static Logger LOGGER = LoggerFactory.getLogger(MutationSet.class.getSimpleName());

  /**
   * @param client
   * @param setNQuads
   * @param delNQuads
   * @return
   */
  public static Map<String, String> mutate(
      DgraphClient client, List<DgraphProto.NQuad> setNQuads, List<DgraphProto.NQuad> delNQuads) {
    if (null == client) {
      LOGGER.warn("null DgraphClient");
      return null;
    }
    if ((null == setNQuads || setNQuads.size() == 0)
        && (null == delNQuads || delNQuads.size() == 0)) {
      LOGGER.debug("no data to mutate");
      return null;
    }

    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation.Builder builder = DgraphProto.Mutation.newBuilder();
      if (null != setNQuads) {
        for (int i = 0; i < setNQuads.size(); i++) {
          builder.addSet(setNQuads.get(i));
        }
      }
      if (null != delNQuads) {
        for (int i = 0; i < delNQuads.size(); i++) {
          System.out.println("toDelete NQuad: " + delNQuads.get(i).toString());
          builder.addDel(delNQuads.get(i));
        }
      }

      DgraphProto.Request request =
          DgraphProto.Request.newBuilder().addMutations(builder.build()).setCommitNow(true).build();
      DgraphProto.Response res = txn.doRequest(request);
      return res.getUidsMap();

    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e.getMessage());
      return null;

    } finally {
      txn.discard();
    }
  }
}
