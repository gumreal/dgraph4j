package io.dgraph.wrapper.query;

import io.dgraph.wrapper.TestBase;
import java.util.*;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryHelperTest extends TestBase {
  @Test
  public void testNodeEdgeCount() {
    try {
      Map<String, Long> map =
          QueryHelper.nodeEdgeCount(getClient(), "country", getCountries(), "geo_has_device");
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
          QueryHelper.nodeEdgeCountSum(getClient(), "country", getCountries(), "geo_has_device");
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
