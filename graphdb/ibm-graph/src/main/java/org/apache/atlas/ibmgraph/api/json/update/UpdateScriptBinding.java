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
package org.apache.atlas.ibmgraph.api.json.update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphVertex;

import com.google.gson.GsonBuilder;

/**
 * Contains the information needed to execute the graph update script and
 * process its results.
 *
 */
public class UpdateScriptBinding {

    private static final String ELEMENT_CHANGES = "elementChanges";
    private static final String NEW_EDGES = "newEdges";
    private static final String NEW_VERTICES = "newVertices";
    private static final String TENANT_ID = "tenantId";

    private List<IBMGraphEdge> newEdges_ = new ArrayList<>();
    private List<IBMGraphVertex> newVertices_ = new ArrayList<>();
    private List<ElementChanges> elementChangesList = new ArrayList<ElementChanges>();
    private String tenantId;

    public UpdateScriptBinding(String tenantId) {
        this.tenantId = tenantId;
    }

    public List<ElementChanges> getElementChangesList() {
        return elementChangesList;
    }

    /**
     * Generates the bindings needed for the update script to apply
     * the changes specified.
     *
     * @return
     */
    public Map<String, Object> getGremlinParameters() {

        List<String> jsonizedNewVertices = new ArrayList<>(newVertices_.size());
        for (IBMGraphVertex newVertex : newVertices_) {
            jsonizedNewVertices.add(newVertex.getIdOrLocalId());
        }

        List<NewEdge> jsonizedNewEdges = new ArrayList<>(newEdges_.size());
        for (IBMGraphEdge edge : newEdges_) {
            NewEdge newEdge = new NewEdge(edge.getIdOrLocalId(), edge.getLabel(),
                    edge.getInVertex().getV().getIdOrLocalId(), edge.getOutVertex().getV().getIdOrLocalId());
            jsonizedNewEdges.add(newEdge);
        }

        Map<String, Object> bindings = new HashMap<>();
        bindings.put(NEW_VERTICES, jsonizedNewVertices);
        bindings.put(NEW_EDGES, jsonizedNewEdges);
        bindings.put(ELEMENT_CHANGES, elementChangesList);
        bindings.put(TENANT_ID, tenantId);
        return bindings;
    }

    public void addNewVertex(IBMGraphVertex vertex) {
        newVertices_.add(vertex);
    }

    //assumes that new vertices with the edge are added separately
    public void addNewEdge(IBMGraphEdge edge) {
        newEdges_.add(edge);
    }

    public boolean hasChanges() {
        return newEdges_.size() > 0 || newVertices_.size() > 0 || elementChangesList.size() > 0;
    }

    public List<IBMGraphVertex> getIncludedNewVertices() {
        return newVertices_;
    }

    public List<IBMGraphEdge> getIncludedNewEdges() {
        return newEdges_;
    }

    @Override
    public String toString() {
        return new GsonBuilder().setPrettyPrinting().create().toJson(getGremlinParameters());
    }

}
