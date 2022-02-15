package io.dgraph.wrapper.mutation;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.Transaction;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mutation {
  private static Logger LOGGER = LoggerFactory.getLogger(MutationSet.class.getSimpleName());

  /**
   * @param client
   * @param setNquads
   * @param delNquads
   * @return
   */
  public static boolean mutate(
      DgraphClient client, List<DgraphProto.NQuad> setNquads, List<DgraphProto.NQuad> delNquads) {
    if (null == client) {
      LOGGER.warn("null DgraphClient");
      return false;
    }
    if ((null == setNquads || setNquads.size() == 0)
        && (null == delNquads || delNquads.size() == 0)) {
      LOGGER.debug("no data to mutate");
      return true;
    }

    Transaction txn = client.newTransaction();
    try {
      DgraphProto.Mutation.Builder builder = DgraphProto.Mutation.newBuilder();
      if (null != setNquads) {
        for (int i = 0; i < setNquads.size(); i++) {
          builder.addSet(setNquads.get(i));
        }
      }
      if (null != delNquads) {
        for (int i = 0; i < delNquads.size(); i++) {
          builder.addDel(delNquads.get(i));
        }
      }
      DgraphProto.Request request =
          DgraphProto.Request.newBuilder().addMutations(builder.build()).setCommitNow(true).build();
      DgraphProto.Response res = txn.doRequest(request);
      LOGGER.debug(res.toString());

    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      return false;

    } finally {
      txn.discard();
    }

    // done
    return true;
  }
}
