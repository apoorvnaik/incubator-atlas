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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.atlas.ibmgraph.api.action.GetVertexDataAction;
import org.apache.atlas.ibmgraph.api.action.IPropertyValue;
import org.apache.atlas.ibmgraph.api.json.JsonEdge;
import org.apache.atlas.ibmgraph.api.json.JsonVertex;
import org.apache.atlas.ibmgraph.api.json.JsonVertexData;
import org.apache.atlas.ibmgraph.api.json.PropertyValue;
import org.apache.atlas.ibmgraph.api.json.update.UpdateScriptBinding;
import org.apache.atlas.ibmgraph.gremlin.expr.GetElementExpression.ElementType;
import org.apache.atlas.ibmgraph.tx.ReadableUpdatedGraphElement;
import org.apache.atlas.ibmgraph.tx.ReadableUpdatedVertex;
import org.apache.atlas.ibmgraph.tx.UpdatedGraphElement;
import org.apache.atlas.ibmgraph.tx.UpdatedVertex;
import org.apache.atlas.ibmgraph.util.AllowedWhenDeleted;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

/**
 * Represents a vertex in the graph.
 *
 */
public class IBMGraphVertex extends IBMGraphElement<JsonVertex> implements AtlasVertex<IBMGraphVertex, IBMGraphEdge> {

    private static final Logger LOG = LoggerFactory.getLogger(IBMGraphVertex.class);

    //the list of outgoing edges, if we know it.  In most cases, the list of outgoing edges is retrieved when
    //we get the vertex.  There are two scenarios when this will be null:
    //1) This vertex is a 'proxy' for an existing vertex in the graph that we have not retrieved the information for yet
    //2) This vertex was created by executing an index query or some other mechanism which does not retrieve the outgoing edges
    private volatile Set<IBMGraphEdge> allOutgoingEdgesInGraph_ = null;

    //used for thread communication, so that other threads known while we are in the process
    //of loading the edges and react appropriately (in most cases by waiting for the loading to finish)
    private volatile boolean isLoadingOutgoingEdges_ = false;

    //list of known incoming edges.  This list is not necessarily complete.  It is updated when the user executes
    //an edge query.  It is kept for bookkeeping so that when
    //the vertex is deleted, the incoming edges that are in the cache can be updated correctly.
    private final Set<IBMGraphEdge> knownIncomingEdgesInGraph_ = new HashSet<>();

    /**
     * Constructor for new vertices
     *
     * @param graph
     * @param label
     * @param initialPropertyValues
     */
    public IBMGraphVertex(IBMGraphGraph graph) {

        super(graph, createLocalId(graph), null, new HashMap<>());
        allOutgoingEdgesInGraph_ = new HashSet<>();
    }

    private static String createLocalId(IBMGraphGraph graph) {
        //use a special character to make it impossible for this to
        //conflict with a real vertex id
        return "@v" + graph.getTx().createNewVertexId() + "@";
    }

    /**
     * Constructor for proxy vertices
     *
     * @param graph
     * @param vertexId
     */
    public IBMGraphVertex(IBMGraphGraph graph, String vertexId) {
        super(graph, vertexId);
    }

    /**
     * Constructor for vertices loaded from the graph
     *
     * @param graph
     * @param info
     */
    public IBMGraphVertex(IBMGraphGraph graph, JsonVertex info) {
        super(graph, info);

    }

    public Set<IBMGraphEdge> getAllOutgoingEdgesInGraph() {

        //If it's a proxy, we should property resolve the proxy (which includes
        //getting the outgoing edges)
        resolveProxyIfNeeded(true);

        //In some cases, such as when running an index query, we cannot automatically retrieve
        //the outgoing edges with the vertex.  In that case, we get them on demand.
        synchronized (this) {
            while (isLoadingOutgoingEdges_) {
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Waiting for outgoing edges to be loaded in {}", toString());
                    }
                    wait();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Finished waiting for outgoing edges to be loaded in {}", toString());
                    }

                } catch (InterruptedException e) {

                }
            }
            if (allOutgoingEdgesInGraph_ != null) {
                return Collections.unmodifiableSet(allOutgoingEdgesInGraph_);
            }
            isLoadingOutgoingEdges_ = true;
        }

        try {
            //Make the rest api call OUTSIDE the synchronized block
            LOG.debug("Loading outgoing edges in {}", toString());
            Collection<JsonEdge> jsonEdges = graph_.getClient().getIncidentEdges(getId().toString(),
                    AtlasEdgeDirection.OUT, null);
            setOutgoingEdges(jsonEdges);
            LOG.debug("Finished loading outgoing edges in {}", toString());
            return Collections.unmodifiableSet(allOutgoingEdgesInGraph_);
        } finally {
            synchronized (this) {
                isLoadingOutgoingEdges_ = false;
                notifyAll();
            }
        }

    }

    public void setOutgoingEdges(Collection<JsonEdge> jsonEdges) {

        if (jsonEdges == null) {
            return;
        }

        Collection edges = graph_.wrapEdges(jsonEdges, false);
        //Note that this relies on the edges returned by wrapEdges being instances of
        //IBMGraphEdge.  That will not be the case if 'true' is passed, and a proxy wrapper is added.

        allOutgoingEdgesInGraph_ = new HashSet<>(jsonEdges.size());
        allOutgoingEdgesInGraph_.addAll(edges);
    }

    @Override
    protected JsonVertex loadJsonInfo(String id) {
        JsonVertexData loaded = graph_.apply(new GetVertexDataAction(this), JsonVertexData.class);
        if (loaded == null) {
            return null;
        }
        setOutgoingEdges(loaded.getOutgoingEdges());
        return loaded.getVertex();
    }



    @Override
    public Iterable<AtlasEdge<IBMGraphVertex, IBMGraphEdge>> getEdges(AtlasEdgeDirection dir, String edgeLabel) {

        Collection<AtlasEdge<IBMGraphVertex, IBMGraphEdge>> result = new HashSet<>();

        if (dir == AtlasEdgeDirection.OUT || dir == AtlasEdgeDirection.BOTH) {
            //We know about all outgoing edges, no need to query the graph
            Collection<IBMGraphEdge> matchingEdges = getMatchingEdges(getAllOutgoingEdgesInTransaction(),
                    edgeLabel);

            //Synchronized to avoid ConcurrentModificationException if this and applyChangesFromTransaction()
            //are called concurrently.  The collection is a computed collection that comes from the outgoing edge
            //list in the vertex.  If that is modified while we are iterating through the list (as is done here, to
            //add all matchingEdges to the result), we get a ConcurrentModificationExeception.
            synchronized (this) {
                result.addAll(matchingEdges);
            }
        }

        if (dir == AtlasEdgeDirection.IN || dir == AtlasEdgeDirection.BOTH) {
            addIncomingEdgesFromGraph(edgeLabel, result);
            Collection<IBMGraphEdge> matchingVertices = getMatchingEdges(getIncomingEdgesAddedInTransaction(),
                    edgeLabel);
            //Synchronized to avoid ConcurrentModificationException if this and applyChangesFromTransaction()
            //are called concurrently.  This collection is also computed, this time from knownIncomingEdgesInGraph_.
            synchronized (this) {
                result.addAll(matchingVertices);
            }
        }

        return result;
    }

    private Collection<IBMGraphEdge> getIncomingEdgesAddedInTransaction() {
        ReadableUpdatedVertex readOnlyUpdatedVertex = getReadOnlyUpdatedVertex();
        if(readOnlyUpdatedVertex == null) {
            return Collections.emptySet();
        }
        return readOnlyUpdatedVertex.getIncomingEdgesAdded();
    }

    private Set<IBMGraphEdge> getAllOutgoingEdgesInTransaction() {
        ReadableUpdatedVertex readOnlyUpdatedVertex = getReadOnlyUpdatedVertex();
        if(readOnlyUpdatedVertex == null) {
            return getAllOutgoingEdgesInGraph();
        }
        return readOnlyUpdatedVertex.getAllOutgoingEdges();
    }

    private void addIncomingEdgesFromGraph(String edgeLabel,
                                           Collection<AtlasEdge<IBMGraphVertex, IBMGraphEdge>> result) {

        if (!isNewElement()) {
            Collection<JsonEdge> incomingEdges = getClient().getIncidentEdges(getIdString(), AtlasEdgeDirection.IN,
                    edgeLabel);
            Collection edges = graph_.wrapEdges(incomingEdges, false);
            //Note that that relies on the wrapped edges coming back as instances of IBMGraphEdge.  That
            //is only the case if 'false' is used as the argument.  Synchronized to avoid ConcurrentModificationException
            //if this is called while getting the list of incoming edges via getEdges()
            synchronized (this) {
                knownIncomingEdgesInGraph_.addAll(edges);
            }
            result.addAll(edges);
        }
    }

    @Override
    public Iterable<AtlasEdge<IBMGraphVertex, IBMGraphEdge>> getEdges(AtlasEdgeDirection d) {

        return getEdges(d, null);
    }

    @Override
    public <T> void addProperty(String propertyName, T value) {

        Object persistentValue = getPersistentPropertyValue(value);
        PropertyValue pv = makePropertyValue(persistentValue);
        updatePropertyValueInMap(propertyName, pv);
    }

    @Override
    protected void updatePropertyValueInMap(String property, IPropertyValue value) {
        if (isMultiProperty(property)) {
            if (value.isIndexed()) {
                Object newValue = value.getValue(value.getOriginalType());
                graph_.getPropertyChangeIndex().onPropertyAdd(this, property, newValue);
            }
            addAdditionalPropertyValueInMap(property, value);
        } else {
            super.updatePropertyValueInMap(property, value);
        }
    }

    //only called for multi-properties
    private void addAdditionalPropertyValueInMap(String name, IPropertyValue newValue) {
        getUpdatedVertex().addAdditionalPropertyValueInMap(name, newValue);

    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.atlas.repository.graphdb.AtlasVertex#getV()
     */
    @Override
    @AllowedWhenDeleted
    public IBMGraphVertex getV() {
        return this;
    }

    @Override
    public <T> Collection<T> getPropertyValues(String propertyName, Class<T> type) {

        Collection<T> result = (Collection<T>) getProperties(type).get(propertyName);
        if (result == null) {
            return Collections.emptyList();
        }
        return result;
    }

    @Override
    public <T> T getWrappedElement() {
        // TODO: Complete implementation
        return null;
    }

    @Override
    public AtlasVertexQuery<IBMGraphVertex, IBMGraphEdge> query() {

        return new IBMGraphVertexQuery(this);
    }

    @Override
    protected ElementType getElementType() {
        return ElementType.VERTEX;
    }

    @Override
    protected boolean isMultiProperty(String propertyName) {
        return graph_.isMultiProperty(propertyName);
    }

    @Override
    protected void createNewElement() {
        UpdateScriptBinding binding = new UpdateScriptBinding(graph_.getTenantId());
        binding.addNewVertex(this);
        graph_.applyUpdates(binding);
    }

    private Collection<IBMGraphEdge> getMatchingEdges(Collection<IBMGraphEdge> edges, final String edgeLabel) {

        return Collections2.filter(edges, new Predicate<IBMGraphEdge>() {

            @Override
            public boolean apply(IBMGraphEdge input) {
                return edgeLabel == null || input.getLabel().equals(edgeLabel);
            }

        });
    }

    public Set<IBMGraphEdge> getKnownIncomingEdgesInGraph() {
        return Collections.unmodifiableSet(knownIncomingEdgesInGraph_);
    }

    @Override
    protected UpdatedGraphElement getUpdatedElement() {
        return getUpdatedVertex();
    }

    private UpdatedVertex getUpdatedVertex() {
        return graph_.getTx().getUpdatedVertex(this);
    }

    @Override
    protected ReadableUpdatedGraphElement getReadOnlyUpdatedElement() {
        return getReadOnlyUpdatedVertex();
    }

    private ReadableUpdatedVertex getReadOnlyUpdatedVertex() {
        return graph_.getTx().getReadOnlyUpdatedVertex(this);
    }

    //Uupdates the global edge collections.  This is called as part of the commit process.  This
    //This is synchronized because calling this at the same time as getEdges() produces a ConcurrentModificationException.
    //Those two operations need to be synchronized.
    public synchronized void applyEdgeChangesFromTransaction(Collection<IBMGraphEdge> outgoingEdgesAdded,
                                                             Collection<IBMGraphEdge> incomingEdgesAdded) {

        if (allOutgoingEdgesInGraph_ != null) {
            applyEdgeChanges(allOutgoingEdgesInGraph_, outgoingEdgesAdded);
        }

        applyEdgeChanges(knownIncomingEdgesInGraph_, incomingEdgesAdded);
    }

    //Removes edges that were deleted and adds the edges that were
    //added to update currentEdges.  The collection is updated in place.
    private void applyEdgeChanges(Collection<IBMGraphEdge> currentEdges, Collection<IBMGraphEdge> edgesAdded) {

        Iterator<IBMGraphEdge> edgeIt = currentEdges.iterator();

        while (edgeIt.hasNext()) {
            IBMGraphEdge edge = edgeIt.next();
            if (edge.isDeleted()) {
                edgeIt.remove();
            }
        }

        currentEdges.addAll(edgesAdded);
    }

    public boolean isOutgoingEdgesLoaded() {
        return allOutgoingEdgesInGraph_ != null;
    }
}
