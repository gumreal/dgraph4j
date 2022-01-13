package io.dgraph.wrapper.mutation;

import io.dgraph.DgraphClient;
import io.dgraph.wrapper.TestBase;
import io.dgraph.wrapper.WrapperException;
import io.dgraph.wrapper.model.VertxBase;
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
    try {
      String uid = MutationSet.setVertx(getClient(), bundle);
      Assert.assertNotNull(uid);

    } catch (WrapperException e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testSetList() {
    List<VertxBase> list = new ArrayList<>();
    list.add(Bundle.newBundle("com.b"));
    list.add(Bundle.newBundle("com.c"));

    try {
      Collection<VertxBase> result = MutationSet.setVertx(getClient(), list);
      Assert.assertNotNull(result);
      Assert.assertEquals(2, result.size());
      result.forEach(dt -> System.out.println("generated [uid]" + dt.getUid()));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testSetJson() {}

  @Test
  public void testSetEdge() {
    String edgeName = "release_in";
    try {
      DgraphClient client = getClient();
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
    }
  }
}
