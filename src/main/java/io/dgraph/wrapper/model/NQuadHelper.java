package io.dgraph.wrapper.model;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto;

/** */
public class NQuadHelper {
  /**
   * @param subjectUid
   * @param prediction
   * @param value
   * @return
   */
  public static DgraphProto.NQuad newNQuadField(
      String subjectUid, String prediction, Object value) {
    if (null == value) {
      return null;
    }

    DgraphProto.Value.Builder valueBuilder = DgraphProto.Value.newBuilder();
    if (value instanceof Integer) {
      valueBuilder.setIntVal((Integer) value);

    } else if (value instanceof Float) {
      valueBuilder.setDoubleVal((Float) value);

    } else if (value instanceof Double) {
      valueBuilder.setDoubleVal((Double) value);

    } else if (value instanceof Boolean) {
      valueBuilder.setBoolVal((Boolean) value);

    } else {
      valueBuilder.setStrVal((String) value);
    }
    return DgraphProto.NQuad.newBuilder()
        .setSubject(subjectUid)
        .setPredicate(prediction)
        .setObjectValue(valueBuilder)
        .build();
  }

  /**
   * @param subjectUid
   * @param prediction
   * @param value
   * @return
   */
  public static DgraphProto.NQuad newNQuadEdge(String subjectUid, String prediction, Object value) {
    return DgraphProto.NQuad.newBuilder()
        .setSubject(subjectUid)
        .setPredicate(prediction)
        .setObjectId((null == value) ? "" : value.toString())
        .build();
  }

  /**
   * @param subjectUid
   * @param prediction
   * @param value
   * @param facets
   * @return
   */
  public static DgraphProto.NQuad newNQuadEdgeWithFacets(
      String subjectUid, String prediction, Object value, DgraphProto.Facet... facets) {
    DgraphProto.NQuad.Builder builder =
        DgraphProto.NQuad.newBuilder()
            .setSubject(subjectUid)
            .setPredicate(prediction)
            .setObjectId((null == value) ? "" : value.toString());

    if (null != facets) {
      for (int i = 0; i < facets.length; i++) {
        builder.addFacets(facets[i]);
      }
    }

    return builder.build();
  }

  /**
   * @param key
   * @param value
   * @return
   */
  public static DgraphProto.Facet newFacet(String key, Object value) {
    return DgraphProto.Facet.newBuilder()
        .setKey(key)
        .setValType(DgraphProto.Facet.ValType.STRING)
        .setValue(ByteString.copyFromUtf8((null == value) ? "" : value.toString()))
        .build();
  }
}
