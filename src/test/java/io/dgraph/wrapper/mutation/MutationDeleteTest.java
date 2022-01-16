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
  String edgeType = "release_in";

  /**
   * @param client
   * @return
   */
  private String[] stubVertexEdges(DgraphClient client) {
    String bundleUid =
        MutationSet.setVertx(client, TestBase.Bundle.newBundle("bundle.testDeleteEdge"));
    String country_1_Uid =
        MutationSet.setVertx(client, TestBase.Country.newCountry("country.01.testDeleteEdge"));
    String country_2_Uid =
        MutationSet.setVertx(client, TestBase.Country.newCountry("country.02.testDeleteEdge"));
    List<String> edges = new ArrayList<>();
    edges.add(country_1_Uid);
    edges.add(country_2_Uid);
    MutationSet.setEdges(client, bundleUid, edgeType, edges);
    return new String[] {bundleUid, country_1_Uid, country_2_Uid};
  }

  @Test
  public void testDeleteEdge() {
    try {
      DgraphClient client = getClient();

      // stub
      String[] uids = stubVertexEdges(client);
      Assert.assertEquals(uids.length, 3);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType, new TestBase.Country()));
      TestBase.Bundle toQuery = new TestBase.Bundle();
      toQuery.setUid(uids[0]);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 2);

      // delete
      MutationDelete.deleteEdge(client, uids[0], edgeType, uids[1]);

      // query again
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 1);

      // TODO clear stub

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testDeleteEdges() {
    try {
      DgraphClient client = getClient();

      // stub
      String[] uids = stubVertexEdges(client);
      Assert.assertEquals(uids.length, 3);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType, new TestBase.Country()));
      TestBase.Bundle toQuery = new TestBase.Bundle();
      toQuery.setUid(uids[0]);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 2);

      // delete
      List<String> toUids = new ArrayList<>();
      toUids.add(uids[1]);
      toUids.add(uids[2]);
      MutationDelete.deleteEdges(client, uids[0], edgeType, toUids);

      // query after delete
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertNull(((TestBase.Bundle) vertex).getRelease_in());

      // TODO clear stub

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testDeleteEdgePredicate() {
    try {
      DgraphClient client = getClient();

      // stub
      String[] uids = stubVertexEdges(client);
      Assert.assertEquals(uids.length, 3);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType, new TestBase.Country()));
      TestBase.Bundle toQuery = new TestBase.Bundle();
      toQuery.setUid(uids[0]);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 2);

      // delete
      MutationDelete.deleteEdgePredicate(client, uids[0], edgeType);

      // query after delete
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertNull(((TestBase.Bundle) vertex).getRelease_in());

      // TODO clear stub

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }
}
