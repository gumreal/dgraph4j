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
  String edgeType1 = "release_in";
  String edgeType2 = "develop_in";

  /**
   * @param client
   * @return
   */
  private String[] stubVertexEdges(DgraphClient client) {
    // vertex
    String bundleUid =
        MutationSet.setVertx(client, TestBase.Bundle.newBundle("bundle.testDeleteEdge"));
    String country_1_Uid =
        MutationSet.setVertx(client, TestBase.Country.newCountry("country.01.testDeleteEdge"));
    String country_2_Uid =
        MutationSet.setVertx(client, TestBase.Country.newCountry("country.02.testDeleteEdge"));

    // edge type 1
    List<String> objectUids = new ArrayList<>();
    objectUids.add(country_1_Uid);
    objectUids.add(country_2_Uid);
    MutationSet.setEdges(client, bundleUid, edgeType1, objectUids);

    // edge type 2
    MutationSet.setEdge(client, bundleUid, edgeType2, country_1_Uid);

    // done with uid array
    String[] resultUids = new String[] {bundleUid, country_1_Uid, country_2_Uid};
    System.out.println(
        String.format(
            "[bundle]%s [country1]%s [country2]%s", bundleUid, country_1_Uid, country_2_Uid));
    return resultUids;
  }

  @Test
  public void testDeleteEdge() {
    DgraphClient client = null;
    try {
      client = getClient();

      // stub
      String[] uids = stubVertexEdges(client);
      Assert.assertEquals(uids.length, 3);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType1, new TestBase.Country()));
      TestBase.Bundle toQuery = new TestBase.Bundle();
      toQuery.setUid(uids[0]);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 2);

      // delete
      MutationDelete.deleteEdge(client, uids[0], edgeType1, uids[1]);

      // query again
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 1);

      // clear stub
      clearStub(client, uids);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  @Test
  public void testDeleteEdges() {
    DgraphClient client = null;
    try {
      client = getClient();

      // stub
      String[] uids = stubVertexEdges(client);
      Assert.assertEquals(uids.length, 3);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType1, new TestBase.Country()));
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
      MutationDelete.deleteEdges(client, uids[0], edgeType1, toUids);

      // query after delete
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertNull(((TestBase.Bundle) vertex).getRelease_in());

      // clear stub
      clearStub(client, uids);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  @Test
  public void testDeleteEdgePredicate() {
    DgraphClient client = null;
    try {
      client = getClient();

      // stub
      String[] uids = stubVertexEdges(client);
      Assert.assertEquals(uids.length, 3);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType1, new TestBase.Country()));
      TestBase.Bundle toQuery = new TestBase.Bundle();
      toQuery.setUid(uids[0]);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 2);

      // delete
      MutationDelete.deleteEdgePredicate(client, uids[0], edgeType1);

      // query after delete
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertNull(((TestBase.Bundle) vertex).getRelease_in());

      // clear stub
      clearStub(client, uids);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  @Test
  public void testDeleteEdgePredicates() {
    DgraphClient client = null;
    try {
      client = getClient();

      // stub
      String[] uids = stubVertexEdges(client);
      Assert.assertEquals(uids.length, 3);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType1, new TestBase.Country()));
      cascadeEdges.add(new CascadeEdge(edgeType2, new TestBase.Country()));
      TestBase.Bundle toQuery = new TestBase.Bundle();
      toQuery.setUid(uids[0]);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 2);
      Assert.assertEquals(((TestBase.Bundle) vertex).getDevelop_in().size(), 1);

      // delete
      List<String> edgePredicates = new ArrayList<>();
      edgePredicates.add(edgeType1);
      edgePredicates.add(edgeType2);
      MutationDelete.deleteEdgePredicates(client, uids[0], edgePredicates);

      // query after delete
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertNull(((TestBase.Bundle) vertex).getRelease_in());
      Assert.assertNull(((TestBase.Bundle) vertex).getDevelop_in());

      // clear stub
      clearStub(client, uids);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  @Test
  public void testDeleteVertx() {
    DgraphClient client = null;
    try {
      client = getClient();

      // stub
      String[] uids = stubVertexEdges(client);
      Assert.assertEquals(uids.length, 3);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType1, new TestBase.Country()));
      cascadeEdges.add(new CascadeEdge(edgeType2, new TestBase.Country()));
      TestBase.Bundle toQuery = new TestBase.Bundle();
      toQuery.setUid(uids[0]);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      Assert.assertTrue(vertex instanceof TestBase.Bundle);
      Assert.assertEquals(((TestBase.Bundle) vertex).getRelease_in().size(), 2);
      Assert.assertEquals(((TestBase.Bundle) vertex).getDevelop_in().size(), 1);

      // delete
      MutationDelete.deleteVertex(client, uids[0]);

      // query after delete
      vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNull(vertex);

      // clear stub
      clearStub(client, uids);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }
}
