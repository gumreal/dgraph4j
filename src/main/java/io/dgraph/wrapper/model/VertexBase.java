package io.dgraph.wrapper.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import io.dgraph.DgraphProto;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Dgraph Custom Data Type */
public abstract class VertexBase implements Serializable {
  protected transient Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

  @SerializedName("dgraph.type")
  private String dgraphType = this.getClass().getSimpleName();

  private String uid;

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

  public String toString() {
    return toJson();
  }

  /**
   * @param jsonStr
   * @return
   */
  public VertexBase transformToJson(String jsonStr) {
    return new Gson().fromJson(jsonStr, this.getClass());
  }

  /**
   * @param jsonStr
   * @return
   */
  public VertexBase mergeJson(String jsonStr) {
    VertexBase vertex = transformToJson(jsonStr);
    if (null != vertex) {
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

  /**
   * get all properties and their values map, for lookup the vertex in dgraph
   *
   * @return
   */
  public abstract Map<String, Object> primaryPairs();

  public void setStubUid() {
    setStubUid(1);
  }

  public void setStubUid(int stubSeq) {
    setUid(String.format("_:%s_%d", getDgraphType(), stubSeq));
  }

  /** @return */
  public boolean isStubUid() {
    return null != getUid() && getUid().startsWith("_:");
  }

  /** @return */
  public List<DgraphProto.NQuad> toNQuadList() {
    List<DgraphProto.NQuad> list = new ArrayList<>();
    list.add(NQuadHelper.newNQuad(getUid(), DataType.DT_DGRAPH_TYPE.toString(), getDgraphType()));

    Map<String, Object> pairs = primaryPairs();
    pairs.forEach(
        (k, v) -> {
          list.add(NQuadHelper.newNQuad(getUid(), k, v));
        });

    return list;
  }

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
