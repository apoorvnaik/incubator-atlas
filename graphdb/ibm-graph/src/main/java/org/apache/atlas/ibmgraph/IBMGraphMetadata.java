/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.ibmgraph;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.atlas.ibmgraph.api.IGraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.PropertyKey;
import org.apache.atlas.ibmgraph.api.json.Schema;
import org.apache.atlas.ibmgraph.api.json.Index.IndexType;
import org.apache.atlas.ibmgraph.api.json.Schema.IndexDefinition;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

/**
 * Abstraction that decouples the graph metadata from the graph itself.  This
 * is needed because is possible to have multiple instances of IBMGraphGraph that
 * point to the same underlying IBM Graph.  This allows the metadata to be stored
 * in one place and shared between those instances.
 */
public class IBMGraphMetadata {

    enum GraphState {
        UNSET,
        UNINITIALIZED,
        INITIALIZING,
        INITIALIZED
    }

    private final String graphName_;

    private volatile GraphState state = GraphState.UNSET;

    //Stores the Property keys that currently exist in the schema for this graph.  These
    //property keys specify the name, datatype, and multiplicity of properties that
    //can be added to vertices and edges.
    private final Map<String, AtlasPropertyKey> existingPropertyKeys_ = new ConcurrentHashMap<String, AtlasPropertyKey>();

    private final Map<String, Index> existingIndices_ = new ConcurrentHashMap<String, Index>();

    public IBMGraphMetadata(String graphName) {
        graphName_ = graphName;
    }

    public String getGraphName() {
        return graphName_;
    }

    public GraphState getState() {
        return state;
    }

    public Map<String, AtlasPropertyKey> getExistingPropertyKeys() {
        return existingPropertyKeys_;
    }

    public Map<String, Index> getExistingIndices() {
        return existingIndices_;
    }

    /**
     * @param initializing
     */
    public void setState(GraphState state) {
        this.state = state;
    }

    /**
     * Gets the definition of the given index, taking into account
     * pending changes from a management transaction.
     *
     * @param name
     * @param pendingIndices
     * @return
     */
    public Index getEffectiveIndex(String name, Map<String, Index> pendingIndices) {

        //synchronize on metadata.getExistingIndices() so that this map does not change while
        //we're doing this, would would cause the logic here to break.
        synchronized(getExistingIndices()) {

            boolean isPendingIndex = pendingIndices.containsKey(name);
            boolean isExistingIndex = getExistingIndices().containsKey(name);

            if (isPendingIndex && ! isExistingIndex) {
                Index def =  pendingIndices.get(name);
                return def;
            } else if (isExistingIndex && ! isPendingIndex) {
                Index def = getExistingIndices().get(name);
                return def;
            }
            else if (isExistingIndex && isPendingIndex) {

                //combine the property keys

                Index existing = getExistingIndices().get(name);
                Index pending =  pendingIndices.get(name);
                Index effectiveIndex = new Index(existing.getName(),
                        existing.isComposite(),
                        existing.isUnique(),
                        existing.requiresReindex(),
                        existing.getType());
                effectiveIndex.addPropertyKeys(existing.getPropertyKeys());
                effectiveIndex.addPropertyKeys(pending.getPropertyKeys());
                return effectiveIndex;
            }
            else {
                return null;
            }
        }

    }

    public void applySchemaUpdates(Collection<Index> pendingIndices, Collection<AtlasPropertyKey> pendingPropertyKeys) {

        synchronized(getExistingIndices()) {
            for(Index pendingIndex : pendingIndices) {
                String name = pendingIndex.getName();
                Index existingIndex = getExistingIndices().get(name);
                if(existingIndex != null) {
                    //add additional property keys to index
                    for(PropertyKey key : pendingIndex.getPropertyKeys()) {
                        if(! existingIndex.getPropertyKeyNames().contains(key.getName())) {
                            existingIndex.addPropertyKey(key);
                        }
                    }
                }
                else {
                    getExistingIndices().put(name, pendingIndex);
                }
            }
        }

        synchronized(getExistingPropertyKeys()) {

            for(AtlasPropertyKey pendingKey : pendingPropertyKeys) {
                if(! getExistingPropertyKeys().containsKey(pendingKey.getName())) {
                    getExistingPropertyKeys().put(pendingKey.getName(), pendingKey);
                }
             }
        }
    }

    public void updateSchemaCache(IGraphDatabaseClient client, AtlasGraphManagement mgmt) {

        Schema schema = client.getSchema();
        updateCachesFromSchema(mgmt, schema);
    }

    public void updateCachesFromSchema(AtlasGraphManagement mgmt, Schema schema) {


        //lock metadata.getExistingPropertyKeys() during the update to prevent
        //reads before the update is complete
        synchronized(getExistingPropertyKeys()) {
            getExistingPropertyKeys().clear();

            for (PropertyKey propertyKey : schema.getPropertyKeys()) {
                getExistingPropertyKeys().put(propertyKey.getName(), new IBMGraphPropertyKey(propertyKey));
            }
        }

        //lock metadata.getExistingIndices() during the update to prevent
        //reads before the update is complete
        synchronized(getExistingIndices()) {
            for (IndexDefinition index : schema.getEdgeIndexes()) {
                getExistingIndices().put(index.getName(),
                        GraphDBUtil.convertIndexDefinition(mgmt, IndexType.EDGE, index));
            }

            for (IndexDefinition index : schema.getVertexIndexes()) {
                getExistingIndices().put(index.getName(),
                        GraphDBUtil.convertIndexDefinition(mgmt, IndexType.VERTEX, index));
            }
        }
    }

}
