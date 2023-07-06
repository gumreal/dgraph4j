package io.dgraph.wrapper;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import io.dgraph.DgraphClient;
import io.dgraph.wrapper.model.VertexBase;
import io.dgraph.wrapper.mutation.MutationDelete;
import java.util.*;

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

  /**
   * @param client
   * @param uids
   */
  protected void clearStub(DgraphClient client, String[] uids) {
    for (int i = 0; i < uids.length; i++) {
      MutationDelete.deleteVertex(client, uids[i]);
    }
  }

  /** */
  public static class Bundle extends VertexBase {
    private String bundleName;
    private List<Country> release_in;
    private List<Country> develop_in;

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
    protected VertexBase mergeChildren(VertexBase other) {
      return null;
    }

    @Override
    public Set<String> getPredicates() {
      if (null == predicates) {
        predicates = new HashSet<>();
        predicates.add("bundleName");
      }
      return predicates;
    }

    private static Set<String> predicates;

    @Override
    public Map<String, Object> primaryPairs() {
      Map<String, Object> map = new HashMap<>();
      map.put("bundleName", getBundleName());
      return map;
    }

    @Override
    public String getBizKey() {
      return null;
    }

    public static Bundle fromJson(String jsonStr) {
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

    public List<Country> getDevelop_in() {
      return develop_in;
    }

    public void setDevelop_in(List<Country> develop_in) {
      this.develop_in = develop_in;
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
    protected VertexBase mergeChildren(VertexBase other) {
      return null;
    }

    @Override
    public Set<String> getPredicates() {
      if (null == predicates) {
        predicates = new HashSet<>();
        predicates.add("country");
      }
      return predicates;
    }

    private static Set<String> predicates;

    @Override
    public Map<String, Object> primaryPairs() {
      Map<String, Object> map = new HashMap<>();
      map.put("country", getCountry());
      return map;
    }

    @Override
    public String getBizKey() {
      return null;
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
