package io.dgraph.wrapper;

import io.dgraph.DgraphClient;
import io.dgraph.wrapper.model.DgraphTypeBase;
import java.util.HashSet;
import java.util.Set;

/** */
public class TestBase {
  protected DgraphClient getClient() throws WrapperException {
    return getClient("localhost");
  }

  protected DgraphClient getClient(String host) throws WrapperException {
    return getClient(host, 9080);
  }

  protected DgraphClient getClient(String host, int port) throws WrapperException {
    return ClientBuilder.newInstance().withAlpha(host, port).build();
  }

  /** */
  public static class Bundle extends DgraphTypeBase {
    private String bundleName;

    /**
     * @param bundleName
     * @return
     */
    public static Bundle newBundle(String bundleName) {
      Bundle b = new Bundle();
      b.setBundleName(bundleName);
      return b;
    }

    @Override
    public Set<String> getPredicates() {
      if (null == predicates) {
        predicates = new HashSet<>();
        predicates.add("bundleName");
      }
      return predicates;
    }

    public String getBundleName() {
      return bundleName;
    }

    public void setBundleName(String bundleName) {
      this.bundleName = bundleName;
    }
  }

  /** */
  public static class Country extends DgraphTypeBase {
    private String country;

    /**
     * @param c
     * @return
     */
    public static Country newCountry(String c) {
      Country country = new Country();
      country.setCountry(c);
      return country;
    }

    @Override
    public Set<String> getPredicates() {
      if (null == predicates) {
        predicates = new HashSet<>();
        predicates.add("country");
      }
      return predicates;
    }

    public String getCountry() {
      return country;
    }

    public void setCountry(String country) {
      this.country = country;
    }
  }
}
