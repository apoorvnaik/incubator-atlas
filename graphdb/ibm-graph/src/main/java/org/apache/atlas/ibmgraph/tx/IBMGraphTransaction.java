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

package org.apache.atlas.ibmgraph.tx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ibmgraph.IBMGraphEdge;
import org.apache.atlas.ibmgraph.IBMGraphGraph;
import org.apache.atlas.ibmgraph.IBMGraphVertex;
import org.apache.atlas.ibmgraph.api.json.update.ElementChanges;
import org.apache.atlas.ibmgraph.api.json.update.UpdateScriptBinding;
import org.apache.atlas.ibmgraph.util.PropertyIndex;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Represents a transaction within the IBM Graph graphdb implementation.  Each thread
 * has its own instance of this class, to provide isolation between different
 * requests.  Here, we keep track of any changes which have been made but not
 * yet pushed into IBM Graph.  We also keep track of new vertices and edges
 * that had to be created prior to the commit in order to meet the application
 * requirements (note that such vertices/edges would not have any properties,
 * since those are set at commit time).  Work is planned to clean this up and
 * keep track of which vertices are new or not by setting a property on the vertex
 * and filter those out of all query results that are returned so that clients do
 * not ever see them.  There is also work planned to add optimistic locking
 * so that transactions do not overwrite each other's changes.
 *
 * At commit time, we generate and execute Gremlin Groovy script which applies
 * the changes to IBM Graph on the server within a single Titan transaction.
 *
 * If the transaction is rolled back, all of the accumulated changes are discarded,
 * and new vertices/edges that were created are removed.
 *
 *
 */
public class IBMGraphTransaction implements GraphTransaction {

    public static final String ENFORCE_TRANSACTION_SEMANTICS_PROPERTY = "atlas.graphdb.ibmgraph.enforceTransactionSemantics";

    //vertices and edges which have been created in the current
    //transaction.  These need to be deleted during rollback.
    private List<IBMGraphVertex> verticesCreated_ = new ArrayList<>();
    private List<IBMGraphEdge> edgesCreated_ = new ArrayList<>();

    //keeps track of property values for new/changed vertices so we can efficiently answer simple
    //graph queries without needing to push the changes into GraphDB.
    private PropertyIndex index_ = new PropertyIndex();


    private boolean enforceTransactionSemantics_ = false;

    //keeps track of changes that have been made to vertices and edges
    //within the context of this transaction
    private Map<IBMGraphVertex, UpdatedVertex> updatedVertices_ = new HashMap<>();
    private Map<IBMGraphEdge, UpdatedEdge> updatedEdges_ = new HashMap<>();

    private IBMGraphGraph parent_;

    private String tId;

    private int nextNewVertexId = 0;
    private int nextNewEdgeId = 0;



    private static final Logger LOG = LoggerFactory.getLogger(IBMGraphTransaction.class);


    private final Exception created = LOG.isDebugEnabled() ? new Exception() : null;

    /**
     * Constructor.
     *
     * @param parent
     */
    public IBMGraphTransaction(IBMGraphGraph parent) {
        try {
            enforceTransactionSemantics_ = ApplicationProperties.get().getBoolean(ENFORCE_TRANSACTION_SEMANTICS_PROPERTY, true);
        }
        catch(AtlasException e) {
            enforceTransactionSemantics_ = true;
        }
        parent_ = parent;
        tId = UUID.randomUUID().toString();
    }

    @Override
    public void rollback() {
        if(hasPendingChanges()) {
            LOG.info(getLogPrefix() +"Rolling back " + toString());
        }
        else {
            LOG.debug(getLogPrefix() +"Rolling back empty transaction " + toString());
        }

        updatedVertices_.clear();
        updatedEdges_.clear();
        index_.clear();

        //delete edges/vertices that were physically created in the graph
        //during the transaction.  These edges and vertices have an id but have no
        //property values set.

        for(IBMGraphEdge edge : edgesCreated_) {

            if(edge.isIdAssigned()) {
                //edge was physically created, need to delete it
                parent_.removeEdge(edge);
            }
            else {
                //edge was not actually created in the graph, but references to it may still be held.  Record
                //that the edge was deleted, so that it will be treated as deleted by new
                //transactions.
                edge.markDeleted();
            }
        }

        for(IBMGraphVertex vertex : verticesCreated_) {

            if(vertex.isIdAssigned()) {
                //vertex was physically created, need to delete it
                parent_.removeVertex(vertex);
            }
            else {
                //vertex was not actually created in the graph, but references to it may still be held.  Record
                //that the edge was deleted, so that it will be treated as deleted by new
                //transactions.
                vertex.markDeleted();
            }
        }

        applyPendingChanges(true);

        //clear these lists, now we've removed the vertices that were created
        //during the transaction
        edgesCreated_.clear();
        verticesCreated_.clear();
    }

    @Override
    public void applyPendingChanges() {
        applyPendingChanges(false);
    }

    private void applyPendingChanges(boolean isCommit) {
        LOG.debug(getLogPrefix() + "Pushing accumulated changing into IBM Graph for: " + toString());
        if (hasPendingChanges()) {
            try {
                getParent().getGraphLock().writeLock().lock();

                if(! isCommit) {
                    if(enforceTransactionSemantics_) {

                        throw new IllegalStateException("Pushing changes to the database before the transaction is committed is disabled");
                    }
                    else {
                        LOG.error(getLogPrefix() + "Pushing changes into the IBM Graph outside the context of a commit.  Caused by:", new Exception());
                    }
                }

                UpdateScriptBinding binding = createUpdateScriptBindings();
                //binding could still be empty if there are pending change -- for example
                //if an edge is added and then removed.
                if (binding.hasChanges()) {
                    getParent().applyUpdates(binding);
                }

                verticesCreated_.clear();
                edgesCreated_.clear();

                for(UpdatedGraphElement updatedElement : updatedVertices_.values()) {
                    updatedElement.pushChangesIntoElement();
                }
                for(UpdatedGraphElement updatedElement : updatedEdges_.values()) {
                    updatedElement.pushChangesIntoElement();
                }
                //element changes are now reflected in the committed graph
                updatedEdges_.clear();
                updatedVertices_.clear();


                //All changes have been committed into the graph now, so the property value queries will now
                //produce correct values.  The index is just so that we can answer those queries prior to
                //pushing the changes into the graph.
                index_.clear();
            }
            finally {
                getParent().getGraphLock().writeLock().unlock();
            }
        }
    }

    private String getLogPrefix() {
        return parent_.getLogPrefix();
    }

    @Override
    public void commit() {
        if(hasPendingChanges()) {
            LOG.info("Committing " + toString());
        }
        else {
            LOG.debug("Committing empty transaction " + toString());
        }
        applyPendingChanges(true);
    }

    @Override
    public AtlasVertex<IBMGraphVertex, IBMGraphEdge> addVertex() {
        IBMGraphVertex result =  new IBMGraphVertex(parent_);
        verticesCreated_.add(result);
        return result;
    }

    @Override
    public AtlasEdge<IBMGraphVertex, IBMGraphEdge> addEdge(IBMGraphVertex outVertex, IBMGraphVertex inVertex, String label) {

        IBMGraphEdge result = new IBMGraphEdge(parent_, outVertex, inVertex, label);

        getUpdatedVertex(outVertex).addOutgoingEdge(result);
        getUpdatedVertex(inVertex).addIncomingEdge(result);
        edgesCreated_.add(result.getE());
        return result;
    }

    @Override
    public PropertyIndex getPropertyChangeIndex() {
        return index_;
    }

    /**
     * Creates binding
     *
     */
    private UpdateScriptBinding createUpdateScriptBindings() {

        UpdateScriptBinding binding = new UpdateScriptBinding(getParent().getTenantId());
        for(IBMGraphVertex vertex : verticesCreated_) {
            //vertex may have been created on demand
            if (vertex.isNewElement() && !vertex.isDeleted()) {
                binding.addNewVertex(vertex);
            }
        }
        for(IBMGraphEdge edge  : edgesCreated_) {
            //edge may have been created on demand
            if (edge.isNewElement() && !edge.isDeleted()) {
                binding.addNewEdge(edge);
            }
        }

        for (UpdatedVertex v : updatedVertices_.values()) {
            ElementChanges vertexChanges = v.convertToElementChanges();
            if (!vertexChanges.isNoOp()) {
                binding.getElementChangesList().add(vertexChanges);
            }
        }

        for (UpdatedEdge e : updatedEdges_.values()) {

            ElementChanges edgeChanges = e.convertToElementChanges();
            if (!edgeChanges.isNoOp()) {
                binding.getElementChangesList().add(edgeChanges);
            }
        }
        return binding;
    }

    @Override
    public UpdatedVertex getUpdatedVertex(IBMGraphVertex vertex) {
        UpdatedVertex result = updatedVertices_.get(vertex);
        if(result == null) {
            result = new DefaultUpdatedVertex(this, vertex);
            updatedVertices_.put(vertex,  result);
        }
        return result;
    }

    @Override
    public UpdatedEdge getUpdatedEdge(IBMGraphEdge edge) {
        UpdatedEdge result = updatedEdges_.get(edge);
        if(result == null) {
            result = new DefaultUpdatedEdge(this, edge);
            updatedEdges_.put(edge,  result);
        }
        return result;
    }

    @Override
    public ReadableUpdatedVertex getReadOnlyUpdatedVertex(IBMGraphVertex vertex) {
        UpdatedVertex foundVertex = updatedVertices_.get(vertex);
        if(foundVertex == null) {
            return null;
        }
        return foundVertex;
    }

    @Override
    public ReadableUpdatedEdge getReadOnlyUpdatedEdge(IBMGraphEdge edge) {
        UpdatedEdge toWrap = updatedEdges_.get(edge);
        if(toWrap == null) {
            return null;
        }
        return toWrap;
    }

    public void removeVertex(AtlasVertex<IBMGraphVertex, IBMGraphEdge> vertex) {

        IBMGraphVertex v = vertex.getV();
        if(v.isDeleted()) {
            return;
        }

        getPropertyChangeIndex().onVertexDeleted(v);

        UpdatedVertex updatedVertex = getUpdatedVertex(v);
        updatedVertex.delete();
    }

    public void removeEdge(AtlasEdge<IBMGraphVertex, IBMGraphEdge> edge) {


        IBMGraphEdge e = edge.getE();

        if(e.isDeleted()) {
            return;
        }

        UpdatedEdge updatedEdge = getUpdatedEdge(e);
        updatedEdge.delete();

    }

    public int createNewVertexId() {
        int result = nextNewVertexId;
        nextNewVertexId++;
        return result;
    }

    public int createNewEdgeId() {
        int result = nextNewEdgeId;
        nextNewEdgeId++;
        return result;
    }

    public String getId() {
        return tId;
    }

    public boolean hasPendingChanges() {
        return updatedEdges_.size() > 0 ||
                updatedVertices_.size() > 0 ||
                edgesCreated_.size() > 0 ||
                verticesCreated_.size() > 0;
    }

    public IBMGraphGraph getParent() {
        return parent_;
    }


    public Throwable getCreationStack() {
        return created;
    }

    @Override
    public String toString() {
        return "IBMGraphTransaction[tId=" + getId() + ", graph=" + parent_.toString() + "]";
    }

}
