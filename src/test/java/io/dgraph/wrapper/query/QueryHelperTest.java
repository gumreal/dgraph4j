package io.dgraph.wrapper.query;

import io.dgraph.DgraphClient;
import io.dgraph.wrapper.TestBase;
import io.dgraph.wrapper.model.VertexBase;
import io.dgraph.wrapper.mutation.MutationSet;
import java.util.*;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryHelperTest extends TestBase {

  @Test
  public void testGetVertx() {
    try {
      DgraphClient client = getClient();

      // stub
      String edgeType = "release_in";
      String bundleUid = MutationSet.setVertx(client, Bundle.newBundle("bundle.test.get.vertx"));
      String country_1_Uid =
          MutationSet.setVertx(client, Country.newCountry("country.01.test.get.vertx"));
      String country_2_Uid =
          MutationSet.setVertx(client, Country.newCountry("country.02.test.get.vertx"));

      List<String> countries = new ArrayList<>();
      countries.add(country_1_Uid);
      countries.add(country_2_Uid);
      MutationSet.setEdges(client, bundleUid, edgeType, countries);

      // query
      List<CascadeEdge> cascadeEdges = new ArrayList<>();
      cascadeEdges.add(new CascadeEdge(edgeType, new Country()));

      Bundle toQuery = new Bundle();
      toQuery.setUid(bundleUid);

      VertexBase vertex = QueryHelper.getVertexByUid(client, toQuery, cascadeEdges);
      Assert.assertNotNull(vertex);
      System.out.println(vertex.toJson());
      Assert.assertTrue(vertex instanceof Bundle);
      Assert.assertEquals(((Bundle) vertex).getRelease_in().size(), 2);

      // reverse query
      cascadeEdges.clear();
      cascadeEdges.add(new CascadeEdge(edgeType, new Bundle(), true));

      Country c1 = new Country();
      c1.setUid(country_1_Uid);

      vertex = QueryHelper.getVertexByUid(client, c1, cascadeEdges);
      Assert.assertNotNull(vertex);
      System.out.println(vertex.toJson());
      Assert.assertTrue(vertex instanceof Country);
      Assert.assertEquals(((Country) vertex).getRelease_in().size(), 1);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testGetByPredicate() {
    try {
      List<String> result = QueryHelper.getUidByPredicate(getClient(), "country", "country_0");
      System.out.println("getUidByPredicate result: " + result.toString());
      Assert.assertNotNull(result);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testNodeEdgeCount() {
    try {
      Map<String, Long> map =
          QueryHelper.vertexEdgeCount(getClient(), "country", getCountries(), "geo_has_device");
      Assert.assertNotNull(map);

      System.out.println("nodeEdgeCount result:");
      map.keySet()
          .stream()
          .forEach(s -> System.out.println(String.format("%s -> %d", s, map.get(s))));
      System.out.println("");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testNodeEdgeCountSum() {
    try {
      Long result =
          QueryHelper.vertxEdgeCountSum(getClient(), "country", getCountries(), "geo_has_device");
      Assert.assertNotNull(result);
      System.out.println(String.format("nodeEdgeCountSum -> %d", result));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testIntersectCount() {
    try {
      Long result = QueryHelper.intersectCount(getClient(), getFilters());
      Assert.assertNotNull(result);
      Assert.assertNotNull(result);
      System.out.println(String.format("intersectCount -> %d", result));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testUnionCount() {
    try {
      Long result = QueryHelper.unionCount(getClient(), getFilters());
      Assert.assertNotNull(result);
      Assert.assertNotNull(result);
      System.out.println(String.format("unionCount -> %d", result));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Set<String> getCountries() {
    Set<String> countries = new HashSet<>();
    countries.add("country_0");
    countries.add("country_1");
    countries.add("country_2");
    countries.add("country_3");
    countries.add("country_4");
    return countries;
  }

  private List<VertxEdgeFilter> getFilters() {
    List<VertxEdgeFilter> filters = new ArrayList<>();
    filters.add(new VertxEdgeFilter("bundle", "com.bundle.0", "install"));
    filters.add(new VertxEdgeFilter("country", "country_0", "geo_has_device"));
    return filters;
  }
}
