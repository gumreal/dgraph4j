package io.dgraph.wrapper.mutation;

import io.dgraph.DgraphClient;
import io.dgraph.wrapper.TestBase;
import io.dgraph.wrapper.WrapperException;
import io.dgraph.wrapper.model.VertexBase;
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
      String uid = MutationSet.setVertx(client, bundle);
      Assert.assertNotNull(uid);

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
      Collection<VertexBase> result = MutationSet.setVertx(client, list);
      Assert.assertNotNull(result);
      Assert.assertEquals(2, result.size());
      result.forEach(dt -> System.out.println("generated [uid]" + dt.getUid()));
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
          MutationSet.setVertx(client, Bundle.newBundle("com." + System.currentTimeMillis()));
      String countryUid = MutationSet.setVertx(client, Country.newCountry("chn"));
      boolean result = MutationSet.setEdge(client, bundleUid, edgeName, countryUid);
      Assert.assertTrue(result);
      System.out.println(
          String.format("[bundle]%s --%s--> [country]%s", bundleUid, edgeName, countryUid));

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
