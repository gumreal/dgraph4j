package io.dgraph.wrapper.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.io.Serializable;
import java.util.Set;

/** Dgraph Custom Data Type */
public abstract class VertexBase implements Serializable {
  @SerializedName("dgraph.type")
  protected String dgraphType = this.getClass().getSimpleName();

  private String uid;

  protected transient Set<String> predicates;

  /** */
  public VertexBase() {}

  /** @param uid */
  public VertexBase(String uid) {
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
  public static VertexBase fromJson(String jsonStr){
    return null;
  }

  /**
   * @param jsonStr
   * @return
   */
  public VertexBase mergeJson(String jsonStr) {
    VertexBase vertex = fromJson(jsonStr);
    if( null != vertex){
      vertex.setUid(getUid());
    }
    return vertex;
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
