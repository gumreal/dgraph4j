package io.dgraph.wrapper.model;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto;

/** */
public class NquadHelper {
  public static DgraphProto.NQuad newNquadEdge(
      String subjectUid, String prediction, String objectUid) {
    return DgraphProto.NQuad.newBuilder()
        .setSubject(subjectUid)
        .setPredicate(prediction)
        .setObjectId(objectUid)
        .build();
  }

  public static DgraphProto.NQuad newNquadEdge(
      String subjectUid, String prediction, String objectUid, DgraphProto.Facet... facets) {
    DgraphProto.NQuad.Builder builder =
        DgraphProto.NQuad.newBuilder()
            .setSubject(subjectUid)
            .setPredicate(prediction)
            .setObjectId(objectUid);

    if (null != facets) {
      for (int i = 0; i < facets.length; i++) {
        builder.addFacets(facets[i]);
      }
    }

    return builder.build();
  }

  public static DgraphProto.Facet newFacet(String key, String value) {
    return DgraphProto.Facet.newBuilder()
        .setKey(key)
        .setValType(DgraphProto.Facet.ValType.STRING)
        .setValue(ByteString.copyFromUtf8(value))
        .build();
  }

  public static DgraphProto.Facet newFacet(String key, Integer value) {
    return DgraphProto.Facet.newBuilder()
        .setKey(key)
        .setValType(DgraphProto.Facet.ValType.INT)
        .setValue(ByteString.copyFromUtf8(value.toString()))
        .build();
  }

  public static DgraphProto.Facet newFacet(String key, Float value) {
    return DgraphProto.Facet.newBuilder()
        .setKey(key)
        .setValType(DgraphProto.Facet.ValType.FLOAT)
        .setValue(ByteString.copyFromUtf8(value.toString()))
        .build();
  }

  public static DgraphProto.Facet newFacet(String key, Boolean value) {
    return DgraphProto.Facet.newBuilder()
        .setKey(key)
        .setValType(DgraphProto.Facet.ValType.BOOL)
        .setValue(ByteString.copyFromUtf8(value.toString()))
        .build();
  }
}
