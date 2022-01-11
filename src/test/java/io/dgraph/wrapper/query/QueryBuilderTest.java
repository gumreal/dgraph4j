package io.dgraph.wrapper.query;

import io.dgraph.DgraphClient;
import io.dgraph.wrapper.ClientBuilder;
import io.dgraph.wrapper.WrapperException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryBuilderTest {
  @Test
  public void testNodeEdgeCount() {
    try {
      Map<String, Long> map =
          QueryBuilder.nodeEdgeCount(getClient(), "country", getCountries(), "geo_has_device");
      Assert.assertNotNull(map);
      map.keySet()
          .stream()
          .forEach(s -> System.out.println(String.format("%s -> %d", s, map.get(s))));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testNodeEdgeCountSum() {
    try {
      Long result =
          QueryBuilder.nodeEdgeCountSum(getClient(), "country", getCountries(), "geo_has_device");
      Assert.assertNotNull(result);
      System.out.println(String.format("sum -> %d", result));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private DgraphClient getClient() throws WrapperException {
    return ClientBuilder.newInstance().withAlpha("192.168.4.235", 9080).build();
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
}
