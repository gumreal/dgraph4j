package io.dgraph.wrapper.mutation;

import io.dgraph.DgraphClient;
import io.dgraph.wrapper.TestBase;
import io.dgraph.wrapper.model.VertexBase;
import io.dgraph.wrapper.query.CascadeEdge;
import io.dgraph.wrapper.query.QueryHelper;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MutationDeleteTest extends TestBase {

  @Test
  public void testDeleteEdge() {
    try {
      DgraphClient client = getClient();

      // stub
      String edgeType = "release_in";
      String bundleUid =
          MutationSet.setVertx(client, TestBase.Bundle.newBundle("bundle.testDeleteEdge"));
      String country_1_Uid =
          MutationSet.setVertx(client, TestBase.Country.newCountry("country.01.testDeleteEdge"));
      MutationSet.setEdge(client, bundleUid, edgeType, country_1_Uid);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType, new TestBase.Country()));

      TestBase.Bundle toQuery = new TestBase.Bundle();
      toQuery.setUid(bundleUid);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 1);

      // delete
      MutationDelete.deleteEdge(client, bundleUid, edgeType, country_1_Uid);

      // query again
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 0);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }
}
