package io.dgraph.wrapper.schema;

import java.io.Serializable;
import java.util.Set;

/**
 * Dgraph Custom Data Type
 */
abstract public class DgraphTypeBase implements Serializable {
    private transient String typeName;
    private String uid;

    /**
     *
     */
    public DgraphTypeBase(){}

    /**
     *
     * @param typeName
     */
    public DgraphTypeBase(String typeName){
        setTypeName(typeName);
    }

    /**
     *
     * @param typeName
     * @param uid
     */
    public DgraphTypeBase(String typeName, String uid){
        setTypeName(typeName);
        setUid(uid);
    }

    /**
     * transform to JSON string for Dgraph Set Command
     *
     * @return
     */
    abstract public String toJson();

    /**
     * get all property names except typeName and uid
     *
     * @return
     */
    abstract public Set<String> getPredicates();

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }
}
