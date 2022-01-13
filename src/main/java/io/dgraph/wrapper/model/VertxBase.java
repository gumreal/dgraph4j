package io.dgraph.wrapper.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.io.Serializable;
import java.util.Set;

/** Dgraph Custom Data Type */
public abstract class VertxBase implements Serializable {
  @SerializedName("dgraph.type")
  protected String dgraphType = this.getClass().getSimpleName();

  private String uid;

  protected transient Set<String> predicates;

  /** */
  public VertxBase() {}

  /** @param uid */
  public VertxBase(String uid) {
    setUid(uid);
  }

  /**
   * transform to JSON string for Dgraph Set Command
   *
   * @return
   */
  public String toJson() {
    return new Gson().toJson(this);
  }

  /**
   * @param jsonStr
   * @return
   */
  public VertxBase fromJson(String jsonStr) {
    return new Gson().fromJson(jsonStr, this.getClass());
  }

  /**
   * @param jsonStr
   * @return
   */
  public VertxBase mergeJson(String jsonStr) {
    VertxBase vertx = fromJson(jsonStr);
    vertx.setUid(getUid());
    return vertx;
  }

  /**
   * get all property names except typeName and uid
   *
   * @return
   */
  public abstract Set<String> getPredicates();

  public String getDgraphType() {
    return dgraphType;
  }

  public void setDgraphType(String dgraphType) {
    this.dgraphType = dgraphType;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }
}
