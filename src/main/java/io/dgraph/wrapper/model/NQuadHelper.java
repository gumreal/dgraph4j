package io.dgraph.wrapper.model;

import com.google.protobuf.ByteString;
import io.dgraph.DgraphProto;

/** */
public class NQuadHelper {
  public static DgraphProto.NQuad newNQuad(String subjectUid, String prediction, Object value) {
    return DgraphProto.NQuad.newBuilder()
        .setSubject(subjectUid)
        .setPredicate(prediction)
        .setObjectId((null == value) ? "" : value.toString())
        .build();
  }

  public static DgraphProto.NQuad newNQuadWithFacets(
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

  public static DgraphProto.Facet newFacet(String key, Object value) {

    return DgraphProto.Facet.newBuilder()
        .setKey(key)
        .setValType(DgraphProto.Facet.ValType.STRING)
        .setValue(ByteString.copyFromUtf8((null == value) ? "" : value.toString()))
        .build();
  }
}
