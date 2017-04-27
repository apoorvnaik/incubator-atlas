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

import org.apache.atlas.ibmgraph.api.json.JsonEdge;
import org.apache.atlas.ibmgraph.api.json.update.UpdateScriptBinding;
import org.apache.atlas.ibmgraph.gremlin.expr.GetElementExpression.ElementType;
import org.apache.atlas.ibmgraph.tx.ReadableUpdatedGraphElement;
import org.apache.atlas.ibmgraph.tx.UpdatedEdge;
import org.apache.atlas.ibmgraph.tx.UpdatedGraphElement;
import org.apache.atlas.ibmgraph.util.AllowedWhenDeleted;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Represents an edge in the graph.
 */
public class IBMGraphEdge extends IBMGraphElement<JsonEdge> implements AtlasEdge<IBMGraphVertex,IBMGraphEdge>{

    private static final Logger LOG = LoggerFactory.getLogger(IBMGraphEdge.class);

    //the in and out vertices for the edge
    private volatile IBMGraphVertex cachedOutVertex_;
    private volatile IBMGraphVertex cachedInVertex_;

    /**
     * Constructor for *new* edges.  These just exist in memory and have
     * not been pushed into the Graph Database yet.
     */
    public IBMGraphEdge(IBMGraphGraph graph, IBMGraphVertex outVertex,
            IBMGraphVertex inVertex, String label) {
        super(graph, createLocalId(graph), label, new HashMap<>());

        cachedOutVertex_ = outVertex;
        cachedInVertex_ = inVertex;
    }

    private static String createLocalId(IBMGraphGraph graph) {
        //use a special character to make it impossible for this to
        //conflict with a real edge id
        return "@e" + graph.getTx().createNewEdgeId() + "@";
    }

    /**
     * Constructor for edges vertices
     *
     * @param graph
     * @param edgeId
     */
    public IBMGraphEdge(IBMGraphGraph graph, String edgeId) {
        super(graph, edgeId);
    }


    /**
     * Constructor for edges loaded from the graph.
     *
     */
    public IBMGraphEdge(IBMGraphGraph parent, JsonEdge edgeInfo) {
        super(parent, edgeInfo);
    }


    @Override
    @AllowedWhenDeleted
    public IBMGraphEdge getE() {
        return this;
    }

    @Override
    @AllowedWhenDeleted
    public AtlasVertex<IBMGraphVertex, IBMGraphEdge> getInVertex() {
        if(cachedInVertex_ == null) {
            resolveProxyIfNeeded(true);
        }
        return cachedInVertex_;
    }

    @Override
    @AllowedWhenDeleted
    public AtlasVertex<IBMGraphVertex, IBMGraphEdge> getOutVertex() {
        if(cachedOutVertex_ == null) {
            resolveProxyIfNeeded(true);
        }
        return cachedOutVertex_;
    }

    @Override
    protected JsonEdge loadJsonInfo(String id) {
        return getClient().getEdge(id);
    }

    @Override
    protected ElementType getElementType() {
        return ElementType.EDGE;
    }

    @Override
    protected void createNewElement() {

        UpdateScriptBinding binding = new UpdateScriptBinding(graph_.getTenantId());

        if(cachedInVertex_.isNewElement()) {
            binding.addNewVertex(cachedInVertex_);
        }

        if(cachedOutVertex_.isNewElement()) {
            binding.addNewVertex(cachedOutVertex_);
        }

        binding.addNewEdge(this);

        graph_.applyUpdates(binding);
    }

    @Override
    protected void setFieldsFromLoadedInfo(JsonEdge loadedInfo) {
        super.setFieldsFromLoadedInfo(loadedInfo);
        //make sure that the in/out vertex fields are set, even if the
        //vertices have been deleted, so we don't violate the assumption
        //that all edges have a non-null in and out vertex
        IBMGraphVertex outVertex = graph_.getVertexWithoutDeleteCheck(loadedInfo.getOutV());
        IBMGraphVertex inVertex = graph_.getVertexWithoutDeleteCheck(loadedInfo.getInV());

        cachedInVertex_ = inVertex.getV();
        cachedOutVertex_ = outVertex.getV();

        //what if the vertex was deleted in other transactions?

        //need to lock both since they will be affected by the delete operation

        if(outVertex.isDeleted() || inVertex.isDeleted()) {
            //one of the vertices has been deleted, so this edge has been implicitly
            //deleted by changes that have not yet been pushed into the graph
            delete();
        }
    }

    @Override
    protected UpdatedGraphElement getUpdatedElement() {
        return getUpdatedEdge();
    }

    private UpdatedEdge getUpdatedEdge() {
        return graph_.getTx().getUpdatedEdge(this);
    }

    @Override
    protected ReadableUpdatedGraphElement getReadOnlyUpdatedElement() {
        return graph_.getTx().getReadOnlyUpdatedEdge(this);
    }

    @Override
    void delete() {
        getUpdatedEdge().delete(null);
    }


    @Override
    public <T> T getWrappedElement() {
        // TODO: Complete this implementation
        return null;
    }
}
