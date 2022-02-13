package io.dgraph.wrapper.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/** Dgraph Custom Data Type */
public abstract class VertexBase implements Serializable {
  protected Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

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
   * @param reader
   * @return
   */
  public String parseDgraphType(JsonReader reader) {
    String dt = null;
    try {
      JsonToken token = reader.peek();
      if (JsonToken.BEGIN_ARRAY == token) {
        reader.beginArray();
        while (reader.hasNext()) {
          if (null == dt) {
            dt = reader.nextString();
          }
        }
        reader.endArray();
      } else if (JsonToken.STRING == token) {
        dt = reader.nextString();
      }
    } catch (Exception e) {
      logger.warn("parseDgraphType EXCEPTION " + e.getMessage());
    }
    return dt;
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
