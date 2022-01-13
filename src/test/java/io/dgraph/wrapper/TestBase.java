package io.dgraph.wrapper;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import io.dgraph.DgraphClient;
import io.dgraph.wrapper.model.VertexBase;
import java.util.HashSet;
import java.util.List;
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
  public static class Bundle extends VertexBase {
    private String bundleName;
    private List<Country> release_in;

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

    public static Bundle fromJson(String jsonStr) {
      System.out.println("Bundle.fromJson");
      return new Gson().fromJson(jsonStr, Bundle.class);
    }

    public String getBundleName() {
      return bundleName;
    }

    public void setBundleName(String bundleName) {
      this.bundleName = bundleName;
    }

    public List<Country> getRelease_in() {
      return release_in;
    }

    public void setRelease_in(List<Country> release_in) {
      this.release_in = release_in;
    }
  }

  /** */
  public static class Country extends VertexBase {
    private String country;

    @SerializedName("~release_in")
    private List<Bundle> release_in;

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

    public static VertexBase fromJson(String jsonStr) {
      return new Gson().fromJson(jsonStr, Country.class);
    }

    public String getCountry() {
      return country;
    }

    public void setCountry(String country) {
      this.country = country;
    }

    public List<Bundle> getRelease_in() {
      return release_in;
    }

    public void setRelease_in(List<Bundle> release_in) {
      this.release_in = release_in;
    }
  }
}
