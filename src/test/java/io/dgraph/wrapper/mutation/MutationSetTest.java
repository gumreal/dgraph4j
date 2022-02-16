package io.dgraph.wrapper.mutation;

import static org.testng.AssertJUnit.assertEquals;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.wrapper.TestBase;
import io.dgraph.wrapper.WrapperException;
import io.dgraph.wrapper.model.NQuadHelper;
import io.dgraph.wrapper.model.VertexBase;
import io.dgraph.wrapper.query.QueryHelper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MutationSetTest extends TestBase {

  @Test
  public void testSetObj() {
    TestBase.Bundle bundle = Bundle.newBundle("com.h");
    bundle.setUid("0x17");

    DgraphClient client = null;
    try {
      client = getClient();
      String uid = MutationSet.setVertex(client, bundle);
      Assert.assertNotNull(uid);

      MutationDelete.deleteVertex(client, uid);

    } catch (WrapperException e) {
      e.printStackTrace();
      Assert.assertTrue(false);

    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  @Test
  public void testSetList() {
    List<VertexBase> list = new ArrayList<>();
    list.add(Bundle.newBundle("com.b"));
    list.add(Bundle.newBundle("com.c"));

    DgraphClient client = null;
    try {
      client = getClient();
      Collection<VertexBase> result = MutationSet.setVertex(client, list);
      Assert.assertNotNull(result);
      Assert.assertEquals(2, result.size());

      for (VertexBase vb : result) {
        System.out.println("generated [uid]" + vb.getUid());
        MutationDelete.deleteVertex(client, vb.getUid());
      }
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
  public void testSetEdge() {
    String edgeName = "release_in";

    DgraphClient client = null;
    try {
      client = getClient();
      String bundleUid =
          MutationSet.setVertex(client, Bundle.newBundle("com." + System.currentTimeMillis()));

      String countryUid = MutationSet.setVertex(client, Country.newCountry("chn"));

      boolean result = MutationSet.setEdge(client, bundleUid, edgeName, countryUid);
      Assert.assertTrue(result);

      System.out.println(
          String.format("[bundle]%s --%s--> [country]%s", bundleUid, edgeName, countryUid));
      MutationDelete.deleteVertex(client, bundleUid);
      MutationDelete.deleteVertex(client, countryUid);

    } catch (WrapperException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }

  @Test
  public void testSetNquads() {
    String edgeName = "release_in";
    DgraphClient client = null;
    try {

      client = getClient();
      // vertex
      String bundleUid =
          MutationSet.setVertex(client, Bundle.newBundle("com." + System.currentTimeMillis()));
      String country1 = MutationSet.setVertex(client, Country.newCountry("chn"));
      String country2 = MutationSet.setVertex(client, Country.newCountry("jpn"));
      System.out.println(
          String.format("[bundle]%s [country1]%s [country2]%s", bundleUid, country1, country2));

      // edge with facets
      List<DgraphProto.NQuad> nquadList = new ArrayList<>();
      nquadList.add(
          NQuadHelper.newNQuadWithFacets(
              bundleUid, edgeName, country1, NQuadHelper.newFacet("weight", new Integer(1))));
      nquadList.add(
          NQuadHelper.newNQuadWithFacets(
              bundleUid, edgeName, country2, NQuadHelper.newFacet("weight", new Integer(2))));
      boolean result = MutationSet.setNquad(client, nquadList);
      Assert.assertTrue(result);

      // query
      int allWeight = QueryHelper.sumFacet(client, bundleUid, edgeName, "weight");
      assertEquals(3, allWeight);

      // delete
      MutationDelete.deleteVertex(client, bundleUid);
      MutationDelete.deleteVertex(client, country1);
      MutationDelete.deleteVertex(client, country2);

    } catch (WrapperException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    } finally {
      if (null != client) {
        client.shutdown();
      }
    }
  }
}
