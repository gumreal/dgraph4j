package io.dgraph.wrapper.mutation;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphProto;
import io.dgraph.wrapper.dic.DataType;
import io.dgraph.wrapper.dic.DateTimeIndexType;
import io.dgraph.wrapper.dic.StringIndexType;

/**
 * Schema Tool
 */
public class MutationSchema {
    /**
     *
     * @param client
     * @param predicate
     * @param dt
     */
    public static void alterSchema(DgraphClient client, String predicate, DataType dt){
        alterSchema(client, predicate, dt, false);
    }

    /**
     *
     * @param client
     * @param predicate
     * @param dt
     */
    public static void alterSchema(DgraphClient client, String predicate, DataType dt, boolean createIndex){
        String indexName = "";
        if(createIndex){
            switch (dt){
                case DT_STRING:
                    indexName = StringIndexType.exact.name();
                    break;
                case DT_DATETIME:
                    indexName = DateTimeIndexType.day.name();
                default:
                    indexName = dt.toString();
            }
            alterSchema(client, predicate, dt, true, indexName);
        }else{
            alterSchema(client, predicate, dt, false, indexName);
        }
    }

    /**
     *
     * @param client
     * @param predicate
     * @param sit
     */
    public static void alterSchema(DgraphClient client, String predicate, StringIndexType sit){
        alterSchema(client, predicate, DataType.DT_STRING, true, sit.name());
    }

    /**
     *
     * @param client
     * @param predicate
     * @param dtit
     */
    public static void alterSchema(DgraphClient client, String predicate, DateTimeIndexType dtit){
        alterSchema(client, predicate, DataType.DT_DATETIME, true, dtit.name());
    }

    /**
     *
     * @param predicate
     * @param dt
     * @param createIndex
     * @param indexName
     * @return
     */
    private static void alterSchema(DgraphClient client, String predicate, DataType dt, boolean createIndex, String indexName) {
        String schema = String.format("%s: %s", predicate, dt.toString());
        if(createIndex){
            schema += String.format(" @index(%s)", indexName);
        }
        DgraphProto.Operation op = DgraphProto.Operation.newBuilder().setSchema(schema).build();
        client.alter(op);
    }
}
